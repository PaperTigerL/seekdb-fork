/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ob_ai_column_dml_hook.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ddl/ob_ai_column_resolver.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "common/object/ob_object.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace share::schema;

/* the sequence which makes the request id of a task group unique and ordered */
static int64_t g_ai_column_request_seq = 0;

int ObAiColumnDmlHook::gen_request_id(ObIAllocator &allocator, ObString &request_id)
{
  int ret = OB_SUCCESS;
  char buf[32];
  const int64_t seq = ObTimeUtility::current_time_us() * 1000
                      + (ATOMIC_FAA_AR(&g_ai_column_request_seq, 1) % 1000);
  const int len = snprintf(buf, sizeof(buf), "%020ld", static_cast<long>(seq));
  if (len <= 0 || len >= static_cast<int>(sizeof(buf))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString tmp(len, buf);
    if (OB_FAIL(ob_write_string(allocator, tmp, request_id))) {
      LOG_WARN("failed to copy the request id", K(ret));
    }
  }
  return ret;
}

/* find the index of the column in the row of the DML */
static int find_col_index(const UIntFixedArray &column_ids, const uint64_t col_id, int64_t &idx)
{
  idx = -1;
  for (int64_t i = 0; i < column_ids.count(); ++i) {
    if (column_ids.at(i) == col_id) {
      idx = i;
      break;
    }
  }
  return OB_SUCCESS;
}

/* whether the column is one of the columns which the AI expression depends on */
static bool is_dep_column(const ObSEArray<uint64_t, 16> &dep_column_ids, const uint64_t col_id)
{
  for (int64_t i = 0; i < dep_column_ids.count(); ++i) {
    if (dep_column_ids.at(i) == col_id) {
      return true;
    }
  }
  return false;
}

/* get the schema of the table which is modified by the DML */
static int get_dml_table_schema(ObTableModifyOp &op,
                                const uint64_t table_id,
                                const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  ObSchemaGetterGuard *schema_guard = op.get_exec_ctx().get_sql_ctx()->schema_guard_;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the schema guard of the DML is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get the schema of the modified table", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("the schema of the modified table is null", K(ret), K(table_id));
  }
  return ret;
}

/* collect the AI columns of the table which is modified by the DML */
static int collect_ai_column_metas(ObTableModifyOp &op,
                                    const uint64_t table_id,
                                    ObIAllocator &allocator,
                                    ObSEArray<ObAiColumnMeta, 16> &metas)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_dml_table_schema(op, table_id, table_schema))) {
  } else if (OB_FAIL(get_table_ai_column_metas(*table_schema, allocator, metas))) {
    LOG_WARN("failed to collect the AI columns of the modified table", K(ret), K(table_id));
  }
  return ret;
}

/* build the text of the rowkey predicate, eg. "`id` = 2".
 * the value of a rowkey column is taken from new_row when the column is one of
 * the assigned columns of the UPDATE, otherwise it is taken from old_row. */
static int build_rowkey_predicate(ObTableModifyOp &op,
                                   const uint64_t table_id,
                                   const UIntFixedArray &column_ids,
                                   const ObExprPtrIArray &old_row,
                                   const ObExprPtrIArray *new_row,
                                   const ColContentFixedArray *assign_columns,
                                   ObIAllocator &allocator,
                                   ObString &predicate)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObSEArray<uint64_t, 8> rowkey_column_ids;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t buf_pos = 0;
  if (OB_FAIL(get_dml_table_schema(op, table_id, table_schema))) {
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_column_ids))) {
    LOG_WARN("failed to get the rowkey columns of the modified table", K(ret), K(table_id));
  } else if (rowkey_column_ids.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the modified table has no rowkey column", K(ret), K(table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); ++i) {
      const uint64_t col_id = rowkey_column_ids.at(i);
      const ObColumnSchemaV2 *col = table_schema->get_column_schema(col_id);
      int64_t idx = -1;
      if (OB_ISNULL(col)) {
        ret = OB_SCHEMA_ERROR;
      } else if (OB_FAIL(find_col_index(column_ids, col_id, idx))) {
      } else if (idx < 0 || idx >= old_row.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the rowkey column is not in the row of the DML", K(ret), K(col_id));
      } else {
        const ObExpr *expr = old_row.at(idx);
        if (OB_NOT_NULL(new_row) && OB_NOT_NULL(assign_columns)
            && OB_NOT_NULL(new_row->at(idx))) {
          /* the value of the rowkey column is only known when it was assigned */
          for (int64_t j = 0; j < assign_columns->count(); ++j) {
            if (assign_columns->at(j).projector_index_ == static_cast<uint64_t>(idx)) {
              expr = new_row->at(idx);
              break;
            }
          }
        }
        ObDatum *datum = nullptr;
        ObObj obj;
        if (OB_FAIL(expr->eval(op.get_eval_ctx(), datum))) {
          LOG_WARN("failed to evaluate the rowkey column", K(ret), K(col_id));
        } else if (OB_FAIL(datum->to_obj(obj, expr->obj_meta_))) {
        } else if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, "`%.*s` = ",
                                           col->get_column_name_str().length(),
                                           col->get_column_name_str().ptr()))) {
        } else if (OB_FAIL(obj.print_sql_literal(buf, buf_len, buf_pos, allocator,
                                                op.get_obj_print_params()))) {
        } else if (i + 1 < rowkey_column_ids.count()
                   && OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, " and "))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf) || buf_pos <= 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObString tmp(buf_pos, buf);
      if (OB_FAIL(ob_write_string(allocator, tmp, predicate))) {
        LOG_WARN("failed to copy the rowkey predicate", K(ret));
      }
    }
  }
  return ret;
}

int ObAiColumnDmlHook::gen_task_insert_stmt(ObTableModifyOp &op,
                                             const uint64_t table_id,
                                             const ObString &request_id,
                                             const ObString &rowkey_predicate,
                                             const ObSEArray<uint64_t, 16> &ai_column_ids,
                                             ObIAllocator &allocator,
                                             ObString &stmt)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t buf_pos = 0;
  ObObj request_id_obj;
  ObObj rowkey_obj;
  request_id_obj.set_varchar(request_id);
  request_id_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  rowkey_obj.set_varchar(rowkey_predicate);
  rowkey_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                              "insert into oceanbase.`%.*s` "
                              "(`table_id`,`column_id`,`request_id`,`rowkey_predicate`,"
                              "`state`,`retry_count`,`eligible_ts`) values ",
                              static_cast<int>(strlen(get_task_table_name())),
                              get_task_table_name()))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_column_ids.count(); ++i) {
      if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, "%s(%ld,%ld,",
                                  0 == i ? "" : ",",
                                  static_cast<long>(table_id),
                                  static_cast<long>(ai_column_ids.at(i))))) {
      } else if (OB_FAIL(request_id_obj.print_sql_literal(buf, buf_len, buf_pos, allocator,
                                                         op.get_obj_print_params()))) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, ","))) {
      } else if (OB_FAIL(rowkey_obj.print_sql_literal(buf, buf_len, buf_pos, allocator,
                                                     op.get_obj_print_params()))) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                                         ",%ld,%ld,now(6))",
                                         static_cast<long>(TASK_STATE_PENDING),
                                         static_cast<long>(0)))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(buf) || buf_pos <= 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObString tmp(buf_pos, buf);
      if (OB_FAIL(ob_write_string(allocator, tmp, stmt))) {
        LOG_WARN("failed to copy the task statement", K(ret));
      }
    }
  }
  return ret;
}

int ObAiColumnDmlHook::gen_task_delete_stmt(ObTableModifyOp &op,
                                             const uint64_t table_id,
                                             const ObString &rowkey_predicate,
                                             ObIAllocator &allocator,
                                             ObString &stmt)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t buf_pos = 0;
  ObObj rowkey_obj;
  rowkey_obj.set_varchar(rowkey_predicate);
  rowkey_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                              "delete from oceanbase.`%.*s` where `table_id` = %ld "
                              "and `rowkey_predicate` = ",
                              static_cast<int>(strlen(get_task_table_name())),
                              get_task_table_name(),
                              static_cast<long>(table_id)))) {
  } else if (OB_FAIL(rowkey_obj.print_sql_literal(buf, buf_len, buf_pos, allocator,
                                                  op.get_obj_print_params()))) {
  } else {
    ObString tmp(buf_pos, buf);
    if (OB_FAIL(ob_write_string(allocator, tmp, stmt))) {
      LOG_WARN("failed to copy the task delete statement", K(ret));
    }
  }
  return ret;
}

/* get the name of the database and of the table which is modified by the DML */
static int get_dml_table_names(ObTableModifyOp &op,
                               const uint64_t table_id,
                               ObIAllocator &allocator,
                               ObString &db_name,
                               ObString &table_name)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_dml_table_schema(op, table_id, table_schema))) {
  } else {
    const ObDatabaseSchema *db_schema = nullptr;
    ObSchemaGetterGuard *schema_guard = op.get_exec_ctx().get_sql_ctx()->schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(schema_guard->get_database_schema(table_schema->get_database_id(),
                                                          db_schema))) {
      LOG_WARN("failed to get the database of the modified table", K(ret), K(table_id));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("the database of the modified table is null", K(ret), K(table_id));
    } else if (OB_FAIL(ob_write_string(allocator, db_schema->get_database_name_str(), db_name))) {
    } else if (OB_FAIL(ob_write_string(allocator, table_schema->get_table_name_str(),
                                       table_name))) {
    }
  }
  return ret;
}

int ObAiColumnDmlHook::gen_result_invalidate_stmt(ObTableModifyOp &op,
                                                   const ObString &db_name,
                                                   const ObString &table_name,
                                                   const ObSEArray<ObString, 16> &ai_column_names,
                                                   const ObString &rowkey_predicate,
                                                   ObIAllocator &allocator,
                                                   ObString &stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t buf_pos = 0;
  if (ai_column_names.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                                     "update `%.*s`.`%.*s` set ",
                                     db_name.length(), db_name.ptr(),
                                     table_name.length(), table_name.ptr()))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_column_names.count(); ++i) {
      if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, "%s`%.*s` = NULL",
                                  0 == i ? "" : ", ",
                                  ai_column_names.at(i).length(),
                                  ai_column_names.at(i).ptr()))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, " where "))) {
    } else if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator, "%.*s",
                                                        rowkey_predicate.length(),
                                                        rowkey_predicate.ptr()))) {
    }
  }
  if (OB_SUCC(ret)) {
    ObString tmp(buf_pos, buf);
    if (OB_FAIL(ob_write_string(allocator, tmp, stmt))) {
      LOG_WARN("failed to copy the invalidation statement", K(ret));
    }
  }
  return ret;
}

int ObAiColumnDmlHook::handle_insert(ObTableModifyOp &op, const ObInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  if (!ins_ctdef.is_primary_index_) {
    /* only the DML of the primary index owns the rows of the table */
    return ret;
  }
  ObIAllocator &allocator = op.get_exec_ctx().get_allocator();
  const uint64_t table_id = ins_ctdef.das_base_ctdef_.index_tid_;
  ObSEArray<ObAiColumnMeta, 16> metas;
  ObSEArray<uint64_t, 16> ai_column_ids;
  if (OB_FAIL(collect_ai_column_metas(op, table_id, allocator, metas))) {
  } else {
    if (metas.count() > 0) {
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(get_dml_table_schema(op, table_id, table_schema))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
          const ObColumnSchemaV2 *col = table_schema->get_column_schema(metas.at(i).column_name_);
          if (OB_ISNULL(col)) {
            ret = OB_SCHEMA_ERROR;
          } else if (OB_FAIL(ai_column_ids.push_back(col->get_column_id()))) {
          }
        }
      }
      if (OB_SUCC(ret) && ai_column_ids.count() > 0) {
        ObString rowkey_predicate;
        ObString request_id;
        ObString stmt;
        if (OB_FAIL(build_rowkey_predicate(op, table_id, ins_ctdef.column_ids_,
                                            ins_ctdef.new_row_, nullptr, nullptr,
                                            allocator, rowkey_predicate))) {
          LOG_WARN("failed to build the rowkey predicate of the inserted row", K(ret), K(table_id));
        } else if (OB_FAIL(gen_request_id(allocator, request_id))) {
        } else if (OB_FAIL(gen_task_insert_stmt(op, table_id, request_id, rowkey_predicate,
                                                ai_column_ids, allocator, stmt))) {
        } else if (OB_FAIL(op.ai_column_pending_stmts_.push_back(stmt))) {
          LOG_WARN("failed to record the task statement", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAiColumnDmlHook::handle_update(ObTableModifyOp &op,
                                     const ObUpdCtDef &upd_ctdef,
                                     ObUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;
  UNUSED(upd_rtdef);
  if (!upd_ctdef.is_primary_index_) {
    return ret;
  }
  ObIAllocator &allocator = op.get_exec_ctx().get_allocator();
  const uint64_t table_id = upd_ctdef.das_base_ctdef_.index_tid_;
  ObSEArray<ObAiColumnMeta, 16> metas;
  if (OB_FAIL(collect_ai_column_metas(op, table_id, allocator, metas))) {
    return ret;
  }
  if (metas.count() <= 0) {
    /* the table has no AI column, nothing has to be invalidated */
    return ret;
  }
  /* a changed rowkey moves the result of the AI column to another key, so the
   * result has to be invalidated and recomputed even when the input does not
   * change */
  const ObTableSchema *table_schema = nullptr;
  ObSEArray<uint64_t, 8> rowkey_ids;
  bool rowkey_changed = false;
  if (OB_FAIL(get_dml_table_schema(op, table_id, table_schema))) {
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_ids))) {
    LOG_WARN("failed to get the rowkey columns of the modified table", K(ret), K(table_id));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && !rowkey_changed
                         && j < upd_ctdef.assign_columns_.count(); ++j) {
      const uint64_t idx = upd_ctdef.assign_columns_.at(j).projector_index_;
      if (idx >= upd_ctdef.column_ids_.count() || idx >= upd_ctdef.old_row_.count()
          || idx >= upd_ctdef.new_row_.count()) {
        continue;
      }
      const uint64_t col_id = upd_ctdef.column_ids_.at(idx);
      bool is_rowkey = false;
      for (int64_t k = 0; !is_rowkey && k < rowkey_ids.count(); ++k) {
        is_rowkey = (rowkey_ids.at(k) == col_id);
      }
      if (!is_rowkey) {
        continue;
      }
      ObDatum *old_datum = nullptr;
      ObDatum *new_datum = nullptr;
      if (OB_FAIL(upd_ctdef.old_row_.at(idx)->eval(op.get_eval_ctx(), old_datum))) {
      } else if (OB_FAIL(upd_ctdef.new_row_.at(idx)->eval(op.get_eval_ctx(), new_datum))) {
      } else if (!ObDatum::binary_equal(*old_datum, *new_datum)) {
        rowkey_changed = true;
      }
    }
  }
  /* the AI columns whose dependency columns were changed */
  ObSEArray<uint64_t, 16> ai_column_ids;
  ObSEArray<ObString, 16> ai_column_names;
  for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
    const ObAiColumnMeta &meta = metas.at(i);
    bool changed = rowkey_changed;
    for (int64_t j = 0; OB_SUCC(ret) && !changed && j < upd_ctdef.assign_columns_.count(); ++j) {
      const uint64_t idx = upd_ctdef.assign_columns_.at(j).projector_index_;
      if (idx >= upd_ctdef.column_ids_.count() || idx >= upd_ctdef.old_row_.count()
          || idx >= upd_ctdef.new_row_.count()) {
        continue;
      }
      const uint64_t col_id = upd_ctdef.column_ids_.at(idx);
      if (!is_dep_column(meta.dep_column_ids_, col_id)) {
        /* the changed column is not a dependency of the AI column */
        continue;
      }
      ObDatum *old_datum = nullptr;
      ObDatum *new_datum = nullptr;
      if (OB_FAIL(upd_ctdef.old_row_.at(idx)->eval(op.get_eval_ctx(), old_datum))) {
      } else if (OB_FAIL(upd_ctdef.new_row_.at(idx)->eval(op.get_eval_ctx(), new_datum))) {
      } else if (!ObDatum::binary_equal(*old_datum, *new_datum)) {
        changed = true;
      }
    }
    if (OB_SUCC(ret) && changed) {
      const ObColumnSchemaV2 *col = table_schema->get_column_schema(meta.column_name_);
      if (OB_ISNULL(col)) {
        ret = OB_SCHEMA_ERROR;
      } else if (OB_FAIL(ai_column_ids.push_back(col->get_column_id()))) {
      } else if (OB_FAIL(ai_column_names.push_back(meta.column_name_))) {
      }
    }
  }
  if (OB_SUCC(ret) && ai_column_ids.count() > 0) {
    ObString old_predicate;
    ObString new_predicate;
    ObString request_id;
    ObString stmt;
    ObString db_name;
    ObString table_name;
    if (OB_FAIL(build_rowkey_predicate(op, table_id, upd_ctdef.column_ids_,
                                        upd_ctdef.old_row_, nullptr, nullptr,
                                        allocator, old_predicate))) {
      LOG_WARN("failed to build the rowkey predicate of the updated row", K(ret), K(table_id));
    } else if (OB_FAIL(build_rowkey_predicate(op, table_id, upd_ctdef.column_ids_,
                                              upd_ctdef.old_row_, &upd_ctdef.new_row_,
                                              &upd_ctdef.assign_columns_,
                                              allocator, new_predicate))) {
      LOG_WARN("failed to build the new rowkey predicate of the updated row", K(ret), K(table_id));
    } else if (OB_FAIL(get_dml_table_names(op, table_id, allocator, db_name, table_name))) {
      LOG_WARN("failed to get the name of the modified table", K(ret), K(table_id));
    } else if (OB_FAIL(gen_request_id(allocator, request_id))) {
    } else if (OB_FAIL(gen_task_insert_stmt(op, table_id, request_id, new_predicate,
                                            ai_column_ids, allocator, stmt))) {
    } else if (OB_FAIL(op.ai_column_pending_stmts_.push_back(stmt))) {
      LOG_WARN("failed to record the task statement", K(ret));
    } else if (old_predicate != new_predicate
               && OB_FAIL(gen_task_delete_stmt(op, table_id, old_predicate, allocator, stmt))) {
      LOG_WARN("failed to build the clean up statement of the old row", K(ret));
    } else if (old_predicate != new_predicate
               && OB_FAIL(op.ai_column_pending_stmts_.push_back(stmt))) {
      LOG_WARN("failed to record the clean up statement", K(ret));
    } else if (OB_FAIL(gen_result_invalidate_stmt(op, db_name, table_name, ai_column_names,
                                                  new_predicate, allocator, stmt))) {
      LOG_WARN("failed to build the invalidation statement", K(ret));
    } else if (OB_FAIL(op.ai_column_pending_stmts_.push_back(stmt))) {
      LOG_WARN("failed to record the invalidation statement", K(ret));
    }
  }
  return ret;
}

int ObAiColumnDmlHook::handle_delete(ObTableModifyOp &op, const ObDelCtDef &del_ctdef)
{
  int ret = OB_SUCCESS;
  if (!del_ctdef.is_primary_index_) {
    return ret;
  }
  ObIAllocator &allocator = op.get_exec_ctx().get_allocator();
  const uint64_t table_id = del_ctdef.das_base_ctdef_.index_tid_;
  ObSEArray<ObAiColumnMeta, 16> metas;
  if (OB_FAIL(collect_ai_column_metas(op, table_id, allocator, metas))) {
    return ret;
  }
  if (metas.count() <= 0) {
    /* the table has no AI column, no task has to be cleaned up */
    return ret;
  }
  ObString rowkey_predicate;
  ObString stmt;
  if (OB_FAIL(build_rowkey_predicate(op, table_id, del_ctdef.column_ids_,
                                      del_ctdef.old_row_, nullptr, nullptr,
                                      allocator, rowkey_predicate))) {
    LOG_WARN("failed to build the rowkey predicate of the deleted row", K(ret), K(table_id));
  } else if (OB_FAIL(gen_task_delete_stmt(op, table_id, rowkey_predicate, allocator, stmt))) {
    LOG_WARN("failed to build the clean up statement of the deleted row", K(ret));
  } else if (OB_FAIL(op.ai_column_pending_stmts_.push_back(stmt))) {
    LOG_WARN("failed to record the clean up statement", K(ret));
  }
  return ret;
}

int ObAiColumnDmlHook::flush(ObTableModifyOp &op)
{
  int ret = OB_SUCCESS;
  if (op.ai_column_pending_stmts_.count() > 0) {
    /* the plan of a plain DML has no nested sql, so the inner connection has to
     * be opened here before the statements of the hook are executed */
    if (OB_FAIL(op.open_inner_conn())) {
      LOG_WARN("failed to open the inner connection of the AI column hook", K(ret));
    } else if (OB_FAIL(op.begin_nested_session(false))) {
      LOG_WARN("failed to begin the nested session of the AI column hook", K(ret));
    } else {
      /* the statements are moved out of the operator before they are executed:
       * the execution of a statement may run another DML which owns the same
       * operator type and must not touch the statements of this hook */
      ObSEArray<ObString, 16> stmts;
      for (int64_t i = 0; OB_SUCC(ret) && i < op.ai_column_pending_stmts_.count(); ++i) {
        if (OB_FAIL(stmts.push_back(op.ai_column_pending_stmts_.at(i)))) {
          LOG_WARN("failed to copy the statement of the AI column hook", K(ret));
        }
      }
      op.ai_column_pending_stmts_.reset();
      ObIAllocator &allocator = op.get_exec_ctx().get_allocator();
      for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
        const ObString &stmt = stmts.at(i);
        /* the inner sql interface expects a null terminated statement */
        char *sql = static_cast<char *>(allocator.alloc(stmt.length() + 1));
        if (OB_ISNULL(sql)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate the statement of the AI column hook", K(ret));
        } else {
          MEMCPY(sql, stmt.ptr(), stmt.length());
          sql[stmt.length()] = '\0';
          const int write_ret = op.execute_write(sql);
          if (OB_SUCCESS != write_ret) {
            /* the inner statement may have been applied even when the nested
             * execution reports an internal error, keep executing the remaining
             * statements */
            LOG_WARN("failed to execute the statement of the AI column hook",
                     K(write_ret), K(stmt));
          }
        }
      }
      const int end_ret = op.end_nested_session();
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end the nested session of the AI column hook", K(end_ret));
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
      }
    }
    op.ai_column_pending_stmts_.reset();
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

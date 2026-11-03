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

#define USING_LOG_PREFIX OBSERVER
#include "observer/virtual_table/ob_all_ai_column.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/resolver/ddl/ob_ai_column_resolver.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;
using namespace sql;

ObAllVirtualAiColumn::ObAllVirtualAiColumn()
  : ObVirtualTableScannerIterator(),
    inited_(false),
    table_idx_(0),
    column_idx_(0),
    table_ids_(),
    cur_table_schema_(NULL)
{
}

ObAllVirtualAiColumn::~ObAllVirtualAiColumn()
{
}

void ObAllVirtualAiColumn::reset()
{
  inited_ = false;
  table_idx_ = 0;
  column_idx_ = 0;
  table_ids_.reset();
  cur_table_schema_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualAiColumn::collect_table_ids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is null", K(ret));
  } else {
    table_ids_.reset();
    if (OB_FAIL(schema_guard_->get_table_ids_in_runtime(table_ids_))) {
      SERVER_LOG(WARN, "failed to get the table ids of the tenant", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualAiColumn::next_ai_column(const ObTableSchema *&table_schema,
                                          const ObColumnSchemaV2 *&column)
{
  int ret = OB_SUCCESS;
  column = NULL;
  table_schema = NULL;
  while (OB_SUCC(ret)) {
    if (OB_ISNULL(cur_table_schema_)) {
      if (table_idx_ >= table_ids_.count()) {
        ret = OB_ITER_END;
        break;
      }
      const uint64_t table_id = table_ids_.at(table_idx_++);
      if (OB_FAIL(schema_guard_->get_table_schema(table_id, cur_table_schema_))) {
        SERVER_LOG(WARN, "failed to get the table schema", K(ret), K(table_id));
        break;
      }
      column_idx_ = 0;
      if (OB_ISNULL(cur_table_schema_)) {
        continue;
      }
    }
    /* find the next AI column of the current table */
    while (column_idx_ < cur_table_schema_->get_column_count()) {
      const ObColumnSchemaV2 *col = cur_table_schema_->get_column_schema_by_idx(column_idx_++);
      if (OB_ISNULL(col) || col->is_hidden() || !sql::is_ai_column(*col)) {
        continue;
      }
      table_schema = cur_table_schema_;
      column = col;
      return ret;
    }
    cur_table_schema_ = NULL;
  }
  return ret;
}

int ObAllVirtualAiColumn::fill_row_cells(const ObTableSchema &table_schema,
                                          const ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == allocator_ || NULL == cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "parameter or data member is not init", K(ret), K(allocator_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells count is less than output column count",
               K(ret), K(cur_row_.count_), K(output_column_ids_.count()));
  } else {
    ObAiColumnMeta meta;
    bool is_ai_meta = false;
    if (OB_FAIL(sql::ai_column_meta_from_comment(column.get_comment_str(), *allocator_,
                                                 meta, is_ai_meta))) {
      SERVER_LOG(WARN, "failed to parse the metadata of the AI column", K(ret));
    } else if (!is_ai_meta) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "the comment of the AI column is invalid", K(ret));
    } else {
      meta.column_name_ = column.get_column_name_str();
      /* the hidden columns of the AI column */
      ObString state_name;
      ObString request_name;
      ObString result_spec_name;
      if (OB_FAIL(sql::get_ai_column_hidden_names(meta.column_name_, *allocator_,
                                                  state_name, request_name, result_spec_name))) {
        SERVER_LOG(WARN, "failed to get the names of the hidden columns", K(ret));
      } else {
        const ObColumnSchemaV2 *state_col = table_schema.get_column_schema(state_name);
        const ObColumnSchemaV2 *request_col = table_schema.get_column_schema(request_name);
        const ObColumnSchemaV2 *result_spec_col = table_schema.get_column_schema(result_spec_name);
        if (OB_NOT_NULL(state_col)) { meta.state_column_id_ = state_col->get_column_id(); }
        if (OB_NOT_NULL(request_col)) { meta.request_column_id_ = request_col->get_column_id(); }
        if (OB_NOT_NULL(result_spec_col)) { meta.result_spec_column_id_ = result_spec_col->get_column_id(); }
      }
      if (OB_SUCC(ret)) {
        const ObCollationType collation_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
        uint64_t cell_idx = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
          const uint64_t col_id = output_column_ids_.at(j);
          ObObj *cell = &cur_row_.cells_[cell_idx];
          switch (col_id) {
          case TABLE_ID: {
            cell->set_int(static_cast<int64_t>(table_schema.get_table_id()));
            break;
          }
          case COLUMN_ID: {
            cell->set_int(static_cast<int64_t>(column.get_column_id()));
            break;
          }
          case COLUMN_NAME: {
            cell->set_varchar(meta.column_name_);
            break;
          }
          case MAX_RETRIES: {
            cell->set_int(meta.max_retries_);
            break;
          }
          case NORMALIZED_EXPR: {
            cell->set_varchar(meta.normalized_expr_);
            break;
          }
          case STATE_COLUMN_ID: {
            cell->set_int(static_cast<int64_t>(meta.state_column_id_));
            break;
          }
          case REQUEST_COLUMN_ID: {
            cell->set_int(static_cast<int64_t>(meta.request_column_id_));
            break;
          }
          case RESULT_SPEC_COLUMN_ID: {
            cell->set_int(static_cast<int64_t>(meta.result_spec_column_id_));
            break;
          }
          case AI_MODEL_NAME: {
            cell->set_varchar(meta.ai_model_name_);
            break;
          }
          case AI_FUNC_TYPE: {
            cell->set_int(meta.ai_func_type_);
            break;
          }
          case SYNC_MODE: {
            cell->set_varchar(meta.sync_mode_);
            break;
          }
          case VECTOR_DIM: {
            if (meta.vector_dim_ < 0) {
              cell->set_null();
            } else {
              cell->set_int(meta.vector_dim_);
            }
            break;
          }
          case RESULT_TYPE: {
            if (meta.result_type_.empty()) {
              cell->set_null();
            } else {
              cell->set_varchar(meta.result_type_);
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
            break;
          }
          }
          if (OB_SUCC(ret)) {
            if (cell->is_string_type()) {
              cell->set_collation_type(collation_type);
            }
            ++cell_idx;
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualAiColumn::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_) || OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "parameter or data member is not init", K(ret));
  } else {
    if (!inited_) {
      if (OB_FAIL(collect_table_ids())) {
      } else {
        inited_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      const ObTableSchema *table_schema = NULL;
      const ObColumnSchemaV2 *column = NULL;
      if (OB_FAIL(next_ai_column(table_schema, column))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SERVER_LOG(WARN, "failed to get the next AI column", K(ret));
        }
      } else if (OB_ISNULL(table_schema) || OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(fill_row_cells(*table_schema, *column))) {
        SERVER_LOG(WARN, "failed to fill the row cells", K(ret));
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

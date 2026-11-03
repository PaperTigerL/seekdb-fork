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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_ai_column_retry_resolver.h"

#include "common/object/ob_obj_type.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "sql/resolver/ddl/ob_ai_column_resolver.h"

namespace oceanbase
{
namespace sql
{

using common::ObString;
using common::ObSqlString;

namespace
{

/* the sequence which makes the request id of a retry unique and ordered */
int64_t g_ai_column_retry_seq = 0;

/* whether the character may be the first character of an identifier */
inline bool is_ident_start(const char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$';
}

/* whether the character may be a character of an identifier */
inline bool is_ident_char(const char c)
{
  return is_ident_start(c) || (c >= '0' && c <= '9');
}

/* whether the two words are the same, the comparison ignores the case */
bool is_same_word(const ObString &word, const char *text)
{
  const int64_t len = strlen(text);
  if (word.length() != len) {
    return false;
  }
  for (int64_t i = 0; i < len; ++i) {
    char c = word.ptr()[i];
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
    if (c != text[i]) {
      return false;
    }
  }
  return true;
}

/* append the name of the column and the value of the column of one row */
int append_rowkey_column(ObSqlString &buf, const share::schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  const common::ObObjType data_type = column.get_data_type();
  /* the predicate text of the row is built at execution time, the text is
   * "`col` = <value>", so the name of the column is a literal and the value is
   * the expression of the column */
  if (OB_FAIL(buf.append("concat('`"))) {
  } else if (OB_FAIL(buf.append(column.get_column_name_str()))) {
  } else if (OB_FAIL(buf.append("` = ', "))) {
  } else if (ob_is_string_tc(data_type)) {
    /* the value of a string column is quoted, it is the text which the worker
     * uses to locate the row */
    if (OB_FAIL(buf.append("quote(`"))) {
    } else if (OB_FAIL(buf.append(column.get_column_name_str()))) {
    } else if (OB_FAIL(buf.append("`))"))) {
    }
  } else if (OB_FAIL(buf.append("`"))) {
  } else if (OB_FAIL(buf.append(column.get_column_name_str()))) {
  } else if (OB_FAIL(buf.append("`)"))) {
  }
  return ret;
}

/* copy the text of the buffer into the allocator */
int copy_text(common::ObIAllocator &allocator, const ObSqlString &sql, ObString &text)
{
  int ret = OB_SUCCESS;
  ObString tmp(sql.length(), sql.ptr());
  if (OB_FAIL(common::ob_write_string(allocator, tmp, text))) {
    LOG_WARN("failed to copy the text", K(ret));
  }
  return ret;
}

} // namespace

int ObAiColumnRetryResolver::gen_request_id(ObIAllocator &allocator, ObString &request_id)
{
  int ret = OB_SUCCESS;
  char buf[32];
  /* the request id is the number of the microsecond, it is ordered, so that the
   * worker can tell the stale tasks from the new ones.  The low digits are
   * reserved for the tasks of the ordinary DML, see gen_request_id() of
   * src/sql/engine/dml/ob_ai_column_dml_hook.cpp, so the request id of a retry
   * is always newer than the request id of a DML of the same microsecond. */
  const int64_t seq = ATOMIC_FAA(&g_ai_column_retry_seq, 1);
  const int64_t value = ObTimeUtility::current_time_us() * 1000 + 500 + (seq % 500);
  const int len = snprintf(buf, sizeof(buf), "%020ld", static_cast<long>(value));
  if (len <= 0 || len >= static_cast<int>(sizeof(buf))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString tmp(len, buf);
    if (OB_FAIL(common::ob_write_string(allocator, tmp, request_id))) {
      LOG_WARN("failed to copy the request id", K(ret));
    }
  }
  return ret;
}

int ObAiColumnRetryResolver::build_rowkey_predicate_expr(const share::schema::ObTableSchema &table_schema,
                                                          ObIAllocator &allocator,
                                                          ObString &rowkey_expr)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  common::ObSEArray<uint64_t, 8> rowkey_column_ids;
  if (OB_FAIL(table_schema.get_rowkey_column_ids(rowkey_column_ids))) {
    LOG_WARN("failed to get the rowkey columns of the table", K(ret));
  } else if (rowkey_column_ids.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the table has no rowkey column", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); ++i) {
      const share::schema::ObColumnSchemaV2 *column =
          table_schema.get_column_schema(rowkey_column_ids.at(i));
      if (OB_ISNULL(column)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("the rowkey column is null", K(ret), K(rowkey_column_ids.at(i)));
      } else if (i > 0 && OB_FAIL(buf.append(" and "))) {
      } else if (OB_FAIL(append_rowkey_column(buf, *column))) {
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(copy_text(allocator, buf, rowkey_expr))) {
  }
  return ret;
}

int ObAiColumnRetryResolver::build_status_expr(const share::schema::ObTableSchema &table_schema,
                                                const uint64_t column_id,
                                                const ObString &ai_column_name,
                                                ObIAllocator &allocator,
                                                ObString &status_expr)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  ObString rowkey_expr;
  if (OB_FAIL(build_rowkey_predicate_expr(table_schema, allocator, rowkey_expr))) {
  } else if (OB_FAIL(buf.append("(case when `"))) {
  } else if (OB_FAIL(buf.append(ai_column_name))) {
  } else if (OB_FAIL(buf.append("` is not null then 'READY' "))) {
  } else if (OB_FAIL(buf.append("when exists (select 1 from oceanbase.`"))) {
  } else if (OB_FAIL(buf.append(OB_AI_COLUMN_TASK_TABLE_NAME))) {
  } else if (OB_FAIL(buf.append("` `__ai_task` "))) {
  } else if (OB_FAIL(buf.append("where `__ai_task`.`table_id` = "))) {
  } else if (OB_FAIL(buf.append_fmt("%ld", static_cast<long>(table_schema.get_table_id())))) {
  } else if (OB_FAIL(buf.append(" and `__ai_task`.`column_id` = "))) {
  } else if (OB_FAIL(buf.append_fmt("%ld", static_cast<long>(column_id)))) {
  } else if (OB_FAIL(buf.append(" and `__ai_task`.`rowkey_predicate` = "))) {
  } else if (OB_FAIL(buf.append(rowkey_expr))) {
  } else if (OB_FAIL(buf.append(") then 'PENDING' else 'FAILED' end)"))) {
  } else if (OB_FAIL(copy_text(allocator, buf, status_expr))) {
  }
  return ret;
}

int ObAiColumnRetryResolver::rewrite_predicate(const ObString &predicate_text,
                                                const ObString &status_expr,
                                                ObIAllocator &allocator,
                                                ObString &rewritten)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  const char *ptr = predicate_text.ptr();
  const int64_t len = predicate_text.length();
  int64_t pos = 0;
  /* whether the identifier which is being read is the second part of a
   * qualified name, such a name is always the ordinary column */
  bool is_qualified = false;
  while (OB_SUCC(ret) && pos < len) {
    const char c = ptr[pos];
    if ('\'' == c || '"' == c) {
      /* the string literal is copied as it is */
      const char quote = c;
      const int64_t start = pos;
      ++pos;
      while (pos < len) {
        if ('\\' == ptr[pos] && pos + 1 < len) {
          pos += 2;
          continue;
        }
        if (quote == ptr[pos]) {
          if (pos + 1 < len && quote == ptr[pos + 1]) {
            pos += 2;
            continue;
          }
          ++pos;
          break;
        }
        ++pos;
      }
      if (OB_FAIL(buf.append(ptr + start, pos - start))) {
        LOG_WARN("failed to copy the string literal", K(ret));
      }
      is_qualified = false;
      continue;
    }
    if ('`' == c) {
      /* the quoted identifier is the ordinary column */
      const int64_t start = pos;
      ++pos;
      while (pos < len && '`' != ptr[pos]) {
        ++pos;
      }
      if (pos < len) {
        ++pos;
      }
      if (OB_FAIL(buf.append(ptr + start, pos - start))) {
        LOG_WARN("failed to copy the quoted identifier", K(ret));
      }
      is_qualified = false;
      continue;
    }
    if (is_ident_start(c)) {
      const int64_t start = pos;
      while (pos < len && is_ident_char(ptr[pos])) {
        ++pos;
      }
      const ObString word(pos - start, ptr + start);
      if (!is_qualified && is_same_word(word, OB_AI_COLUMN_STATUS_NAME)) {
        /* the STATUS pseudo column is replaced by the expression of the state */
        if (OB_FAIL(buf.append(status_expr))) {
          LOG_WARN("failed to copy the status expression", K(ret));
        }
      } else if (OB_FAIL(buf.append(ptr + start, pos - start))) {
        LOG_WARN("failed to copy the identifier", K(ret));
      }
      is_qualified = false;
      continue;
    }
    if ('.' == c) {
      is_qualified = true;
    } else if (' ' != c && '\t' != c && '\n' != c && '\r' != c) {
      is_qualified = false;
    }
    if (OB_FAIL(buf.append(&c, 1))) {
      LOG_WARN("failed to copy the character", K(ret));
    }
    ++pos;
  }
  if (OB_SUCC(ret) && OB_FAIL(copy_text(allocator, buf, rewritten))) {
  }
  return ret;
}

int ObAiColumnRetryResolver::resolve_retry_ai_column(const share::schema::ObTableSchema &table_schema,
                                                      const ObString &database_name,
                                                      const ObString &ai_column_name,
                                                      const ObString &predicate_text,
                                                      ObIAllocator &allocator,
                                                      ObAiColumnRetryInfo &info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObColumnSchemaV2 *ai_column = table_schema.get_column_schema(ai_column_name);
  ObString status_expr;
  ObString rewritten;
  ObString request_id;
  ObString rowkey_expr;
  ObSqlString invalidate_buf;
  ObSqlString task_buf;
  if (OB_ISNULL(ai_column)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
  } else if (!is_ai_column(*ai_column)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
  } else if (OB_FAIL(build_status_expr(table_schema, ai_column->get_column_id(),
                                        ai_column_name, allocator, status_expr))) {
  } else if (OB_FAIL(rewrite_predicate(predicate_text, status_expr, allocator, rewritten))) {
  } else if (OB_FAIL(gen_request_id(allocator, request_id))) {
  } else if (OB_FAIL(build_rowkey_predicate_expr(table_schema, allocator, rowkey_expr))) {
  } else if (OB_FAIL(invalidate_buf.append("update `"))) {
  } else if (OB_FAIL(invalidate_buf.append(database_name))) {
  } else if (OB_FAIL(invalidate_buf.append("`.`"))) {
  } else if (OB_FAIL(invalidate_buf.append(table_schema.get_table_name_str()))) {
  } else if (OB_FAIL(invalidate_buf.append("` set `"))) {
  } else if (OB_FAIL(invalidate_buf.append(ai_column_name))) {
  } else if (OB_FAIL(invalidate_buf.append("` = null where "))) {
  } else if (OB_FAIL(invalidate_buf.append(rewritten.empty() ? "1 = 1" : rewritten))) {
  } else if (OB_FAIL(copy_text(allocator, invalidate_buf, info.invalidate_sql_))) {
  } else if (OB_FAIL(task_buf.append("insert into oceanbase.`"))) {
  } else if (OB_FAIL(task_buf.append(OB_AI_COLUMN_TASK_TABLE_NAME))) {
  } else if (OB_FAIL(task_buf.append("` (`table_id`,`column_id`,`request_id`,`rowkey_predicate`,`state`,`retry_count`,`eligible_ts`) "))) {
  } else if (OB_FAIL(task_buf.append_fmt("select %ld, %ld, concat('%.*s', ",
                                          static_cast<long>(table_schema.get_table_id()),
                                          static_cast<long>(ai_column->get_column_id()),
                                          request_id.length(), request_id.ptr()))) {
  } else if (OB_FAIL(task_buf.append(rowkey_expr))) {
  } else if (OB_FAIL(task_buf.append("), "))) {
  } else if (OB_FAIL(task_buf.append(rowkey_expr))) {
  } else if (OB_FAIL(task_buf.append(", 0, 0, now(6) from `"))) {
  } else if (OB_FAIL(task_buf.append(database_name))) {
  } else if (OB_FAIL(task_buf.append("`.`"))) {
  } else if (OB_FAIL(task_buf.append(table_schema.get_table_name_str()))) {
  } else if (OB_FAIL(task_buf.append("` where "))) {
  } else if (OB_FAIL(task_buf.append(rewritten.empty() ? "1 = 1" : rewritten))) {
  } else if (OB_FAIL(copy_text(allocator, task_buf, info.task_sql_))) {
  }
  if (OB_SUCC(ret)) {
    info.table_id_ = table_schema.get_table_id();
    info.column_id_ = ai_column->get_column_id();
    info.ai_column_name_ = ai_column_name;
    info.request_id_ = request_id;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

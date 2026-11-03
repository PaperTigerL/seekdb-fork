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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RETRY_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RETRY_RESOLVER_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{

/**
 * The state of one AI column of one row.
 *
 * The values are the values of the STATUS pseudo column.  They are aligned
 * with the states of the tasks of oceanbase.__all_ai_column_task, see the
 * constants TASK_STATE_PENDING and TASK_STATE_RETRYING of
 * src/sql/engine/dml/ob_ai_column_dml_hook.h and of
 * src/observer/ai_column/ob_ai_column_worker.cpp :
 *
 *   PENDING  the task of the row waits to be processed by the worker
 *   RUNNING  the request of the row is being sent to the AI model
 *   READY    the result of the row is published
 *   RETRYING the request of the row is sent again
 *   FAILED   the request of the row failed, the result of the row stays null
 */
enum class ObAiColumnState : int64_t
{
  PENDING  = 0,
  RUNNING  = 1,
  READY    = 2,
  RETRYING = 3,
  FAILED   = 4,
};

/* the name of the STATUS pseudo column, it holds the state of the AI column */
#define OB_AI_COLUMN_STATUS_NAME         "status"

/* the name of the task table, it is an inner table of the oceanbase database */
#define OB_AI_COLUMN_TASK_TABLE_NAME     "__all_ai_column_task"

/**
 * The info of one RETRY AI COLUMN action.
 *
 * The action invalidates the results of one AI column of the rows which match
 * the predicate and it creates one task of oceanbase.__all_ai_column_task for
 * every invalidated row, so the worker sends the requests again.
 */
struct ObAiColumnRetryInfo
{
  ObAiColumnRetryInfo()
    : table_id_(0),
      column_id_(0),
      ai_column_name_(),
      request_id_(),
      invalidate_sql_(),
      task_sql_()
  {}
  /* the id of the table which owns the AI column */
  uint64_t table_id_;
  /* the id of the AI column */
  uint64_t column_id_;
  /* the name of the AI column */
  common::ObString ai_column_name_;
  /* the prefix of the request id of the tasks, see gen_request_id() */
  common::ObString request_id_;
  /* the statement which invalidates the results of the AI column */
  common::ObString invalidate_sql_;
  /* the statement which creates the tasks of the AI column */
  common::ObString task_sql_;
};

/**
 * The resolver of the RETRY AI COLUMN action.
 *
 * The action is resolved into two statements which are executed by the
 * executor of the ALTER TABLE statement :
 *
 *   update `db`.`tbl` set `col` = null where <predicate>
 *   insert into oceanbase.`__all_ai_column_task` (...) select ... from `db`.`tbl` where <predicate>
 *
 * The predicate is the text of the WHERE clause of the action.  The STATUS
 * pseudo column of the predicate is replaced by the expression which holds the
 * state of the AI column of the row, so that the predicate may select the rows
 * by the state of their AI column.
 */
class ObAiColumnRetryResolver
{
public:
  /**
   * Resolve the RETRY AI COLUMN action.
   *
   * The column must be an AI column of the table, otherwise the action is
   * rejected with OB_NOT_SUPPORTED.
   */
  static int resolve_retry_ai_column(const share::schema::ObTableSchema &table_schema,
                                      const common::ObString &database_name,
                                      const common::ObString &ai_column_name,
                                      const common::ObString &predicate_text,
                                      common::ObIAllocator &allocator,
                                      ObAiColumnRetryInfo &info);

  /**
   * Build the expression of the STATUS pseudo column.
   *
   * The expression is the state of the AI column of the row, it is
   *   READY    if the result of the row is not null
   *   PENDING  if the result of the row is null and the row owns a task
   *   FAILED   if the result of the row is null and the row owns no task
   */
  static int build_status_expr(const share::schema::ObTableSchema &table_schema,
                                const uint64_t column_id,
                                const common::ObString &ai_column_name,
                                common::ObIAllocator &allocator,
                                common::ObString &status_expr);

  /**
   * Build the expression of the rowkey predicate of one row, eg. "`id` = 2".
   *
   * The text of the expression is the text which the worker uses to locate the
   * row, so it is the same text as the rowkey_predicate of the tasks.
   */
  static int build_rowkey_predicate_expr(const share::schema::ObTableSchema &table_schema,
                                          common::ObIAllocator &allocator,
                                          common::ObString &rowkey_expr);

  /**
   * Replace the STATUS pseudo column of the predicate by its expression.
   *
   * Only the unquoted identifier "status" is the pseudo column, the quoted
   * identifier `status` is the ordinary column of the table.
   */
  static int rewrite_predicate(const common::ObString &predicate_text,
                                const common::ObString &status_expr,
                                common::ObIAllocator &allocator,
                                common::ObString &rewritten);

  /* build the request id of the tasks of one retry */
  static int gen_request_id(common::ObIAllocator &allocator, common::ObString &request_id);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RETRY_RESOLVER_H_

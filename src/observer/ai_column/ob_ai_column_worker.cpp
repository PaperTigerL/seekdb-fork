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

#define USING_LOG_PREFIX SERVER
#include "observer/ai_column/ob_ai_column_worker.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/object/ob_object.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ai_service/ob_ai_service_proxy.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/resolver/ddl/ob_ai_column_resolver.h"

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
using namespace sql;

namespace
{

const char *const TASK_TABLE_NAME = "__all_ai_column_task";
const char *const JSON_SYSTEM_PROMPT = "Return exactly one valid JSON value.";
const char *const INT_SYSTEM_PROMPT = "Return the result as a base-10 integer.";
const char *const DEFAULT_SYSTEM_PROMPT = "You are a helpful assistant.";

/* the state of a task which waits to be processed */
const int64_t TASK_STATE_PENDING = 0;
/* the state of a task which is being retried */
const int64_t TASK_STATE_RETRYING = 3;

/* the result of the request of one AI column */
struct ObAiColumnResult
{
  ObAiColumnResult()
    : column_id_(0), column_name_(), db_name_(), table_name_(), ai_model_name_(),
      ai_func_type_(0), max_retries_(0), prompt_(), result_(), is_success_(false), error_msg_()
  {}
  int64_t column_id_;
  ObString column_name_;
  ObString db_name_;
  ObString table_name_;
  ObString ai_model_name_;
  int64_t ai_func_type_;
  int64_t max_retries_;
  /* the text of the request which is sent to the AI model */
  ObString prompt_;
  /* the text of the answer of the AI model */
  ObString result_;
  bool is_success_;
  ObString error_msg_;
};

/* whether the type of the column is an integer type */
bool is_integer_type(const ObObjType type)
{
  switch (type) {
  case ObTinyIntType:
  case ObSmallIntType:
  case ObMediumIntType:
  case ObInt32Type:
  case ObIntType:
  case ObUTinyIntType:
  case ObUSmallIntType:
  case ObUMediumIntType:
  case ObUInt32Type:
  case ObUInt64Type:
    return true;
  default:
    return false;
  }
}

/* build the system prompt of the request, it depends on the type of the result */
ObString build_system_prompt(const ObColumnSchemaV2 &column)
{
  if (column.is_json()) {
    return ObString(JSON_SYSTEM_PROMPT);
  } else if (is_integer_type(column.get_data_type())) {
    return ObString(INT_SYSTEM_PROMPT);
  } else {
    return ObString(DEFAULT_SYSTEM_PROMPT);
  }
}

/* whether the text is the text of a number */
bool is_number_text(const ObString &text)
{
  bool has_digit = false;
  for (int64_t i = 0; i < text.length(); ++i) {
    const char c = text.ptr()[i];
    if (c >= '0' && c <= '9') {
      has_digit = true;
    } else if (c != '-' && c != '+' && c != '.') {
      return false;
    }
  }
  return has_digit;
}

/* build the literal of the value of the result column */
int gen_result_literal(ObIAllocator &allocator,
                       const ObColumnSchemaV2 &column,
                       const ObString &result,
                       ObString &literal)
{
  int ret = OB_SUCCESS;
  if (is_integer_type(column.get_data_type())) {
    if (!is_number_text(result)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("the answer of the AI model is not a number", K(ret), K(result));
    } else {
      literal = result;
    }
  } else {
    ObObj obj;
    obj.set_varchar(result);
    obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    char *buf = nullptr;
    int64_t buf_len = 0;
    int64_t buf_pos = 0;
    ObObjPrintParams print_params;
    if (OB_FAIL(obj.print_sql_literal(buf, buf_len, buf_pos, allocator, print_params))) {
    } else {
      ObString tmp(buf_pos, buf);
      if (OB_FAIL(ob_write_string(allocator, tmp, literal))) {
      }
    }
  }
  return ret;
}

/* send the request of one AI column to the AI model */
int call_ai_model(ObArenaAllocator &allocator,
                  ObISQLClient &sql_client,
                  const ObString &ai_model_name,
                  const int64_t ai_func_type,
                  const ObString &system_prompt,
                  const ObString &user_prompt,
                  ObString &result)
{
  int ret = OB_SUCCESS;
  share::ObAiModelEndpointInfo endpoint;
  const ObNameCaseMode name_case_mode = OB_ORIGIN_AND_INSENSITIVE;
  if (OB_FAIL(share::ObAiServiceProxy::select_ai_endpoint_by_ai_model_name(
              allocator, sql_client, ai_model_name, name_case_mode, endpoint))) {
    LOG_WARN("failed to get the endpoint of the AI model", K(ret), K(ai_model_name));
  } else {
    ObString access_key;
    ObString model_name = endpoint.get_request_model_name();
    /* the provider modifies the prompts, so it needs writable strings */
    ObString sys_prompt(system_prompt);
    ObString usr_prompt(user_prompt);
    ObArray<ObString> headers;
    ObJsonObject *body = nullptr;
    ObJsonObject *response = nullptr;
    ObAIFuncClient client;
    if (OB_FAIL(endpoint.get_unencrypted_access_key(allocator, access_key))) {
      LOG_WARN("failed to get the access key of the AI endpoint", K(ret));
    } else if (ai_func_type == static_cast<int64_t>(ObAiFuncType::DENSE_EMBEDDING)) {
      ObAIFuncIEmbed *embed_provider = nullptr;
      ObArray<ObString> contents;
      ObIJsonBase *result_base = nullptr;
      if (OB_FAIL(ObAIFuncUtils::get_embed_provider(allocator, endpoint.get_provider(),
                                                    embed_provider))) {
      } else if (OB_FAIL(embed_provider->get_header(allocator, access_key, headers))) {
      } else if (OB_FAIL(contents.push_back(usr_prompt))) {
      } else if (OB_FAIL(embed_provider->get_body(allocator, model_name, contents, nullptr, body))) {
      } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
      } else if (OB_FAIL(embed_provider->parse_output(allocator, response, result_base))) {
      } else {
        /* the answer of the embedding model is the array of the embeddings of
         * the inputs, the request of one AI column owns one input */
        ObIJsonBase *embedding = result_base;
        if (OB_NOT_NULL(result_base)
            && ObJsonNodeType::J_ARRAY == result_base->json_type()) {
          ObJsonArray *result_array = static_cast<ObJsonArray *>(result_base);
          if (1 == result_array->element_count()) {
            embedding = result_array->get_value(0);
          }
        }
        if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, embedding, result))) {
        }
      }
    } else {
      ObAIFuncIComplete *complete_provider = nullptr;
      ObIJsonBase *result_base = nullptr;
      if (OB_FAIL(ObAIFuncUtils::get_complete_provider(allocator, endpoint.get_provider(),
                                                      complete_provider))) {
      } else if (OB_FAIL(complete_provider->get_header(allocator, access_key, headers))) {
      } else if (OB_FAIL(complete_provider->get_body(allocator, model_name, sys_prompt,
                                                     usr_prompt, nullptr, body))) {
      } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
      } else if (OB_FAIL(complete_provider->parse_output(allocator, response, result_base))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, result))) {
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to send the request to the AI model", K(ret), K(ai_model_name));
    }
  }
  return ret;
}

/* evaluate the input expression of the AI column on the row of the task */
int eval_input_expr(ObIAllocator &allocator,
                    ObISQLClient &sql_client,
                    const ObString &db_name,
                    const ObString &table_name,
                    const ObString &input_expr,
                    const ObString &rowkey_predicate,
                    ObString &prompt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT %.*s FROM `%.*s`.`%.*s` WHERE %.*s",
                              input_expr.length(), input_expr.ptr(),
                              db_name.length(), db_name.ptr(),
                              table_name.length(), table_name.ptr(),
                              rowkey_predicate.length(), rowkey_predicate.ptr()))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
        LOG_WARN("failed to evaluate the expression of the AI column", K(ret));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          /* the row of the task does not exist anymore */
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("failed to read the value of the expression of the AI column", K(ret));
      } else {
        ObString value;
        const int64_t value_idx = 0;
        if (OB_FAIL(result->get_varchar(value_idx, value))) {
          LOG_WARN("failed to get the value of the expression of the AI column", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, value, prompt))) {
          LOG_WARN("failed to copy the value of the expression of the AI column", K(ret));
        }
      }
      /* the result must be closed before the transaction uses the connection again */
      const int close_ret = res.close();
      if (OB_SUCCESS != close_ret && OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  return ret;
}

/* whether a newer task of the same AI column and of the same row exists */
int check_stale(ObIAllocator &allocator,
                ObISQLClient &sql_client,
                const uint64_t table_id,
                const int64_t column_id,
                const ObString &rowkey_predicate,
                const ObString &request_id,
                bool &is_stale)
{
  int ret = OB_SUCCESS;
  is_stale = false;
  ObSqlString sql;
  ObObj rowkey_obj;
  ObObj request_id_obj;
  rowkey_obj.set_varchar(rowkey_predicate);
  rowkey_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  request_id_obj.set_varchar(request_id);
  request_id_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t buf_pos = 0;
  ObObjPrintParams print_params;
  if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                              "select count(*) from oceanbase.`%s` where `table_id` = %ld "
                              "and `column_id` = %ld and `rowkey_predicate` = ",
                              TASK_TABLE_NAME,
                              static_cast<long>(table_id),
                              static_cast<long>(column_id)))) {
  } else if (OB_FAIL(rowkey_obj.print_sql_literal(buf, buf_len, buf_pos,
                                                  allocator, print_params))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, buf_pos, allocator,
                                     " and `request_id` > "))) {
  } else if (OB_FAIL(request_id_obj.print_sql_literal(buf, buf_len, buf_pos,
                                                     allocator, print_params))) {
  } else {
    buf[buf_pos++] = '\0';
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_client.read(res, buf))) {
        LOG_WARN("failed to check the stale tasks", K(ret));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        int64_t count = 0;
        const int64_t count_idx = 0;
        if (OB_FAIL(result->get_int(count_idx, count))) {
          LOG_WARN("failed to get the number of the stale tasks", K(ret));
        } else {
          is_stale = count > 0;
        }
      }
      /* the result must be closed before the transaction uses the connection again */
      const int close_ret = res.close();
      if (OB_SUCCESS != close_ret && OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  return ret;
}

} // namespace

ObAiColumnWorker::ObAiColumnWorker()
  : timer_(), sql_proxy_(nullptr), is_inited_(false)
{
}

ObAiColumnWorker::~ObAiColumnWorker()
{
}

int ObAiColumnWorker::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObAiColumnWorker::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(timer_.init("AiColumnWorker", ObMemAttr("AiColumnWorker")))) {
    LOG_WARN("failed to init the timer of the AI column worker", K(ret));
  } else if (OB_FAIL(timer_.schedule(*this, WORK_PERIOD_US, true, true))) {
    LOG_WARN("failed to schedule the AI column worker", K(ret));
  }
  return ret;
}

void ObAiColumnWorker::stop()
{
  if (timer_.inited()) {
    timer_.stop();
  }
}

void ObAiColumnWorker::destroy()
{
  if (timer_.inited()) {
    timer_.destroy();
  }
}

void ObAiColumnWorker::runTimerTask()
{
  const int ret = do_work();
  if (OB_SUCCESS != ret) {
    LOG_WARN("the AI column worker failed", K(ret));
  }
}

int ObAiColumnWorker::do_work()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSEArray<ObAiColumnTaskInfo, 32> tasks;
  if (OB_FAIL(poll_tasks(allocator, tasks))) {
    LOG_WARN("failed to poll the tasks of the AI columns", K(ret));
    ret = OB_SUCCESS;
  } else {
    int64_t begin = 0;
    while (OB_SUCC(ret) && begin < tasks.count()) {
      int64_t end = begin;
      while (end < tasks.count() && tasks.at(end).request_id_ == tasks.at(begin).request_id_) {
        ++end;
      }
      if (OB_FAIL(process_task_group(allocator, tasks, begin, end))) {
        LOG_WARN("failed to process the group of the AI column tasks", K(ret));
        ret = OB_SUCCESS;
      }
      begin = end;
    }
  }
  return ret;
}

int ObAiColumnWorker::poll_tasks(ObIAllocator &allocator,
                                 ObSEArray<ObAiColumnTaskInfo, 32> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt(
      "select `table_id`,`column_id`,`request_id`,`rowkey_predicate`,`state`,`retry_count`,"
      "ifnull(`request_payload`,'') from oceanbase.`%s` "
      "where `state` in (%ld,%ld) and `eligible_ts` <= now(6) "
      "order by `request_id` limit %ld",
      TASK_TABLE_NAME,
      static_cast<long>(TASK_STATE_PENDING),
      static_cast<long>(TASK_STATE_RETRYING),
      static_cast<long>(MAX_TASK_NUM)))) {
    LOG_WARN("failed to build the statement which polls the tasks", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("failed to read the tasks of the AI columns", K(ret));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret) && OB_SUCC(ret = result->next())) {
          ObAiColumnTaskInfo task;
          int64_t table_id = 0;
          int64_t column_id = 0;
          int64_t state = 0;
          int64_t retry_count = 0;
          ObString request_id;
          ObString rowkey_predicate;
          ObString request_payload;
          const int64_t idx_table_id = 0;
          const int64_t idx_column_id = 1;
          const int64_t idx_request_id = 2;
          const int64_t idx_rowkey_predicate = 3;
          const int64_t idx_state = 4;
          const int64_t idx_retry_count = 5;
          const int64_t idx_request_payload = 6;
          if (OB_FAIL(result->get_int(idx_table_id, table_id))) {
          } else if (OB_FAIL(result->get_int(idx_column_id, column_id))) {
          } else if (OB_FAIL(result->get_varchar(idx_request_id, request_id))) {
          } else if (OB_FAIL(result->get_varchar(idx_rowkey_predicate, rowkey_predicate))) {
          } else if (OB_FAIL(result->get_int(idx_state, state))) {
          } else if (OB_FAIL(result->get_int(idx_retry_count, retry_count))) {
          } else if (OB_FAIL(result->get_varchar(idx_request_payload, request_payload))) {
          } else {
            task.table_id_ = static_cast<uint64_t>(table_id);
            task.column_id_ = column_id;
            task.state_ = state;
            task.retry_count_ = retry_count;
            if (OB_FAIL(ob_write_string(allocator, request_id, task.request_id_))) {
            } else if (OB_FAIL(ob_write_string(allocator, rowkey_predicate,
                                               task.rowkey_predicate_))) {
            } else if (OB_FAIL(ob_write_string(allocator, request_payload,
                                               task.request_payload_))) {
            } else if (OB_FAIL(tasks.push_back(task))) {
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
      /* the result must be closed before the connection is used again */
      const int close_ret = res.close();
      if (OB_SUCCESS != close_ret && OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  return ret;
}

int ObAiColumnWorker::process_task_group(ObArenaAllocator &allocator,
                                          ObSEArray<ObAiColumnTaskInfo, 32> &tasks,
                                          const int64_t begin,
                                          const int64_t end)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAiColumnResult, 8> results;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(schema_service->get_runtime_schema_guard(schema_guard))) {
    LOG_WARN("failed to get the schema guard of the AI column worker", K(ret));
    return ret;
  }
  /* the metadata of the AI columns of the group and the text of their requests */
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    ObAiColumnTaskInfo &task = tasks.at(i);
    const ObTableSchema *table_schema = nullptr;
    ObSEArray<ObAiColumnMeta, 16> metas;
    ObAiColumnResult result;
    result.column_id_ = task.column_id_;
    const int schema_ret = schema_guard.get_table_schema(task.table_id_, table_schema);
    if (OB_FAIL(schema_ret)) {
      ret = schema_ret;
      LOG_WARN("failed to get the schema of the table of the AI column", K(ret),
               K(task.table_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("the schema of the table of the AI column is null", K(ret), K(task.table_id_));
    } else if (OB_FAIL(get_table_ai_column_metas(*table_schema, allocator, metas))) {
      LOG_WARN("failed to get the metadata of the AI columns", K(ret), K(task.table_id_));
    } else {
      const ObAiColumnMeta *meta = nullptr;
      for (int64_t j = 0; j < metas.count(); ++j) {
        const ObColumnSchemaV2 *col = table_schema->get_column_schema(metas.at(j).column_name_);
        if (OB_NOT_NULL(col) && col->get_column_id() == static_cast<uint64_t>(task.column_id_)) {
          meta = &metas.at(j);
          result.column_name_ = metas.at(j).column_name_;
          result.ai_model_name_ = metas.at(j).ai_model_name_;
          result.ai_func_type_ = metas.at(j).ai_func_type_;
          result.max_retries_ = metas.at(j).max_retries_;
          result.db_name_ = table_schema->get_table_name_str();
          break;
        }
      }
      if (OB_ISNULL(meta)) {
        /* the AI column does not exist anymore, the task is obsolete */
        ret = OB_SUCCESS;
        LOG_WARN("the AI column of the task does not exist anymore", K(task.table_id_),
                 K(task.column_id_));
        break;
      }
      const ObDatabaseSchema *db_schema = nullptr;
      if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), db_schema))) {
        LOG_WARN("failed to get the database of the AI column", K(ret));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_SCHEMA_ERROR;
      } else {
        ObString db_name;
        if (OB_FAIL(ob_write_string(allocator, db_schema->get_database_name_str(), db_name))) {
        } else {
          result.db_name_ = db_name;
          result.table_name_ = table_schema->get_table_name_str();
        }
      }
      if (OB_SUCC(ret)) {
        if (!task.request_payload_.empty()) {
          /* the request was already sent, the very same request is sent again */
          if (OB_FAIL(ob_write_string(allocator, task.request_payload_, result.prompt_))) {
          }
        } else if (OB_FAIL(eval_input_expr(allocator, *sql_proxy_, result.db_name_, result.table_name_,
                                           meta->input_expr_, task.rowkey_predicate_,
                                           result.prompt_))) {
          LOG_WARN("failed to evaluate the expression of the AI column", K(ret));
          result.error_msg_ = "failed to evaluate the input expression";
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(results.push_back(result))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    return ret;
  }
  /* the requests of the AI models, they are sent without holding a transaction */
  for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    ObAiColumnResult &result = results.at(i);
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(tasks.at(begin).table_id_, table_schema))) {
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
    } else {
      const ObColumnSchemaV2 *col = table_schema->get_column_schema(result.column_name_);
      if (OB_ISNULL(col)) {
        ret = OB_SCHEMA_ERROR;
      } else {
        ObString system_prompt = build_system_prompt(*col);
        ObString ai_result;
        const int call_ret = call_ai_model(allocator, *sql_proxy_, result.ai_model_name_,
                                           result.ai_func_type_, system_prompt,
                                           result.prompt_, ai_result);
        if (OB_SUCCESS != call_ret) {
          result.is_success_ = false;
          result.error_msg_ = "failed to send the request to the AI model";
        } else if (OB_FAIL(ob_write_string(allocator, ai_result, result.result_))) {
          result.is_success_ = false;
          result.error_msg_ = "failed to copy the answer of the AI model";
        } else {
          result.is_success_ = true;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    return ret;
  }
  /* the results of the group are published together */
  ObMySQLTransaction apply_trans;
  if (OB_FAIL(apply_trans.start(sql_proxy_))) {
    LOG_WARN("failed to start the transaction which publishes the results", K(ret));
    return ret;
  }
  bool is_stale = false;
  bool has_failure = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    ObAiColumnResult &result = results.at(i);
    if (!result.is_success_) {
      has_failure = true;
      continue;
    }
    if (OB_FAIL(check_stale(allocator, *sql_proxy_, tasks.at(begin).table_id_, result.column_id_,
                            tasks.at(begin + i).rowkey_predicate_,
                            tasks.at(begin + i).request_id_, is_stale))) {
      LOG_WARN("failed to check the stale tasks", K(ret));
    } else if (is_stale) {
      /* a newer request of the same row exists, the late answer is dropped */
      LOG_WARN("the answer of the AI model is stale, it is dropped", K(result.column_id_),
               K(tasks.at(begin + i).request_id_));
      has_failure = true;
    }
  }
  if (OB_SUCC(ret) && !has_failure) {
    /* one statement publishes all the results of the group */
    ObSqlString sql;
    ObArray<ObString> literals;
    for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
      ObAiColumnResult &result = results.at(i);
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tasks.at(begin).table_id_, table_schema))) {
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_SCHEMA_ERROR;
      } else {
        const ObColumnSchemaV2 *col = table_schema->get_column_schema(result.column_name_);
        ObString literal;
        if (OB_ISNULL(col)) {
          ret = OB_SCHEMA_ERROR;
        } else if (OB_FAIL(gen_result_literal(allocator, *col, result.result_, literal))) {
          LOG_WARN("failed to build the literal of the result", K(ret));
        } else if (OB_FAIL(literals.push_back(literal))) {
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt("update `%.*s`.`%.*s` set ",
                                                results.at(0).db_name_.length(),
                                                results.at(0).db_name_.ptr(),
                                                results.at(0).table_name_.length(),
                                                results.at(0).table_name_.ptr()))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < literals.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%s`%.*s` = %.*s", 0 == i ? "" : ", ",
                                   results.at(i).column_name_.length(),
                                   results.at(i).column_name_.ptr(),
                                   literals.at(i).length(), literals.at(i).ptr()))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(" where %.*s",
                                                 tasks.at(begin).rowkey_predicate_.length(),
                                                 tasks.at(begin).rowkey_predicate_.ptr()))) {
      } else {
        int64_t affected_rows = 0;
        const int publish_write_ret = apply_trans.write(sql.ptr(), affected_rows);
        if (OB_SUCCESS != publish_write_ret) {
          /* the result stays unpublished, every task of the group is retried */
          has_failure = true;
          LOG_WARN("failed to publish the results of the AI columns", K(publish_write_ret));
        }
      }
    }
  }
  /* the tasks of the group are removed, they are either published or dropped */
  for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    ObAiColumnResult &result = results.at(i);
    ObAiColumnTaskInfo &task = tasks.at(begin + i);
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (result.is_success_ && !has_failure) {
      if (OB_FAIL(sql.assign_fmt("delete from oceanbase.`%s` where `table_id` = %ld "
                                  "and `column_id` = %ld and `request_id` = '%.*s'",
                                  TASK_TABLE_NAME,
                                  static_cast<long>(task.table_id_),
                                  static_cast<long>(task.column_id_),
                                  task.request_id_.length(), task.request_id_.ptr()))) {
      } else if (OB_FAIL(apply_trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("failed to remove the task of the AI column", K(ret));
      }
    } else if (task.retry_count_ < result.max_retries_) {
      /* the request is sent again, the very same request text is used */
      if (OB_FAIL(sql.assign_fmt("update oceanbase.`%s` set `state` = %ld, "
                                  "`retry_count` = `retry_count` + 1, "
                                  "`eligible_ts` = date_add(now(6), interval 1 second), "
                                  "`last_error` = '%.*s', `request_payload` = '%.*s' "
                                  "where `table_id` = %ld and `column_id` = %ld and `request_id` = '%.*s'",
                                  TASK_TABLE_NAME,
                                  static_cast<long>(TASK_STATE_RETRYING),
                                  result.error_msg_.length(), result.error_msg_.ptr(),
                                  result.prompt_.length(), result.prompt_.ptr(),
                                  static_cast<long>(task.table_id_),
                                  static_cast<long>(task.column_id_),
                                  task.request_id_.length(), task.request_id_.ptr()))) {
      } else if (OB_FAIL(apply_trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("failed to schedule the retry of the task of the AI column", K(ret));
      }
    } else {
      /* the retries are used up, the result of the AI column stays null */
      if (OB_FAIL(sql.assign_fmt("delete from oceanbase.`%s` where `table_id` = %ld "
                                  "and `column_id` = %ld and `request_id` = '%.*s'",
                                  TASK_TABLE_NAME,
                                  static_cast<long>(task.table_id_),
                                  static_cast<long>(task.column_id_),
                                  task.request_id_.length(), task.request_id_.ptr()))) {
      } else if (OB_FAIL(apply_trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("failed to remove the task of the AI column", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
    }
  }
  const int publish_ret = apply_trans.end(OB_SUCC(ret));
  if (OB_SUCCESS != publish_ret) {
    LOG_WARN("failed to commit the transaction which publishes the results", K(publish_ret));
    if (OB_SUCCESS == ret) {
      ret = publish_ret;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

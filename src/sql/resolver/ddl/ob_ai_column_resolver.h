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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RESOLVER_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{

/**
 * The AI generation function which is bound to an AI column.
 * The value is persisted in oceanbase.__all_ai_column.ai_func_type.
 */
enum class ObAiFuncType : int64_t
{
  COMPLETION = 0,
  DENSE_EMBEDDING = 1,
};

/* The suffixes of the three hidden columns which are created for every AI column. */
#define OB_AI_COLUMN_STATE_SUFFIX       "__ai_state"
#define OB_AI_COLUMN_REQUEST_SUFFIX      "__ai_request"
#define OB_AI_COLUMN_RESULT_SPEC_SUFFIX  "__ai_result_spec"

/**
 * The marker of the AI column metadata.
 *
 * The metadata of an AI column is persisted in the comment of the AI column, so
 * that it survives a schema reload and can be exposed by the
 * oceanbase.__all_ai_column virtual table without any extra inner table write.
 */
#define OB_AI_COLUMN_META_PREFIX         "OB_AI_COLUMN_META_V1"

/* The prompt which is added to the normalized expression of a json typed AI column. */
#define OB_AI_COLUMN_JSON_PROMPT         "Return exactly one valid JSON value."

/* The only supported sync mode of an AI column. */
#define OB_AI_COLUMN_SYNC_MODE_ASYNC     "async"

/* The byte length of the hidden request column of an AI column. */
#define OB_AI_COLUMN_REQUEST_LENGTH      4096

/* The byte length of the hidden result spec column of an AI column. */
#define OB_AI_COLUMN_RESULT_SPEC_LENGTH  1024

/**
 * The metadata of one AI column.
 *
 * The persistent part of the metadata is stored in the comment of the AI column,
 * see ai_column_meta_to_comment() and ai_column_meta_from_comment().  The
 * dependency columns and the ids of the hidden columns are resolved from the
 * table schema, see get_table_ai_column_metas().
 */
struct ObAiColumnMeta
{
  ObAiColumnMeta()
    : column_name_(),
      ai_model_name_(),
      input_expr_(),
      normalized_expr_(),
      sync_mode_(),
      result_type_(),
      user_comment_(),
      ai_func_type_(static_cast<int64_t>(ObAiFuncType::COMPLETION)),
      max_retries_(0),
      vector_dim_(-1),
      state_column_id_(0),
      request_column_id_(0),
      result_spec_column_id_(0),
      dep_column_ids_(),
      dep_column_names_()
  {}

  /* the name of the user visible AI column */
  common::ObString column_name_;
  /* the name of the AI model which generates the column */
  common::ObString ai_model_name_;
  /* the input expression text of the AI column, it is the text between the
   * parentheses of AI_COMPLETE/AI_EMBED */
  common::ObString input_expr_;
  /* the normalized expression text, it is the prompt which is sent to the AI model */
  common::ObString normalized_expr_;
  /* the sync mode of the AI column, only 'async' is supported now */
  common::ObString sync_mode_;
  /* the type name of the result column, eg. 'json', 'varchar', 'vector' */
  common::ObString result_type_;
  /* the comment which the user specified on the AI column */
  common::ObString user_comment_;
  /* ObAiFuncType */
  int64_t ai_func_type_;
  /* the max retry count of the AI task */
  int64_t max_retries_;
  /* the dimension of the embedding, -1 means that the column has no dimension */
  int64_t vector_dim_;
  /* the column id of the hidden state column, it is resolved from the table schema */
  uint64_t state_column_id_;
  /* the column id of the hidden request column, it is resolved from the table schema */
  uint64_t request_column_id_;
  /* the column id of the hidden result spec column, it is resolved from the table schema */
  uint64_t result_spec_column_id_;
  /* the ordinary columns which the AI expression depends on */
  common::ObSEArray<uint64_t, 16> dep_column_ids_;
  /* the names of the ordinary columns which the AI expression depends on */
  common::ObSEArray<common::ObString, 16> dep_column_names_;
};

/**
 * Serialize the metadata of an AI column into the text which is stored in the
 * comment of the AI column.
 */
int ai_column_meta_to_comment(const ObAiColumnMeta &meta,
                               common::ObIAllocator &allocator,
                               common::ObString &comment);

/**
 * Parse the metadata of an AI column back from the comment of the AI column.
 * is_ai_meta is set to false when the comment does not hold any AI column
 * metadata, the meta is left untouched in that case.
 */
int ai_column_meta_from_comment(const common::ObString &comment,
                                 common::ObIAllocator &allocator,
                                 ObAiColumnMeta &meta,
                                 bool &is_ai_meta);

/* Whether the comment holds the metadata of an AI column. */
bool is_ai_column_meta(const common::ObString &comment);

/* Whether the column is an AI column. */
bool is_ai_column(const share::schema::ObColumnSchemaV2 &column);

/**
 * Collect the AI columns of one table.
 *
 * This is the interface which the DML side uses to find out the AI columns of a
 * table, the hidden columns which belong to them and the ordinary columns which
 * they depend on.  Only the AI columns which depend on the changed columns have
 * to be invalidated, so the dependency set is the key of the invalidation.
 */
int get_table_ai_column_metas(const share::schema::ObTableSchema &table_schema,
                               common::ObIAllocator &allocator,
                               common::ObIArray<ObAiColumnMeta> &metas);

/**
 * Build the names of the three hidden columns of an AI column.
 * The names are '<column_name>__ai_state', '<column_name>__ai_request' and
 * '<column_name>__ai_result_spec'.
 */
int get_ai_column_hidden_names(const common::ObString &column_name,
                                common::ObIAllocator &allocator,
                                common::ObString &state_name,
                                common::ObString &request_name,
                                common::ObString &result_spec_name);

/**
 * Collect the columns which are referenced by the input expression of an AI
 * column.  The identifiers of the expression are matched against the columns of
 * the table, the string literals and the function names are skipped.
 */
int collect_ai_expr_dep_columns(const common::ObString &input_expr,
                                const share::schema::ObTableSchema &table_schema,
                                common::ObIAllocator &allocator,
                                const uint64_t exclude_column_id,
                                common::ObSEArray<uint64_t, 16> &dep_column_ids);

/**
 * Fill the schema of one hidden column of an AI column.
 * The hidden column is not visible to the user and it holds the runtime state,
 * the request payload or the result spec of the AI column.
 */
int fill_ai_hidden_column(share::schema::ObColumnSchemaV2 &column,
                           const common::ObString &column_name,
                           const common::ObObjType data_type,
                           const int64_t data_length);

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_AI_COLUMN_RESOLVER_H_

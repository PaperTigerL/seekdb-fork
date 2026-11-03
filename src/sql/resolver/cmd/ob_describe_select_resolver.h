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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_DESCRIBE_SELECT_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_DESCRIBE_SELECT_RESOLVER_

#include "sql/resolver/dml/ob_select_resolver.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace sql
{

/**
 * @file ob_describe_select_resolver.h
 *
 * Resolver of `describe <select statement>`.
 *
 * The syntax is borrowed from duckdb, the statement reports the schema of the
 * result of the select statement:
 *
 *   column_name | column_type | null | key | default | extra
 *
 * The inner select statement is resolved first, so that the inferred schema of
 * the file tables (read_csv/read_json/read_parquet/glob) is taken into account.
 * The describe result is then built as a plain select statement over a
 * generated union of constant rows, the very same trick that is used by the
 * show resolver (see ObShowResolver::parse_and_resolve_select_sql).
 */
class ObDescribeSelectResolver : public ObSelectResolver
{
public:
  explicit ObDescribeSelectResolver(ObResolverParams &params) : ObSelectResolver(params) {}
  virtual ~ObDescribeSelectResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDescribeSelectResolver);

  /* resolve the inner select statement, the schema is taken from the result */
  int resolve_inner_select(const ParseNode &select_node, ObSelectStmt *&inner_stmt);
  /* build the sql that returns the describe result */
  int gen_describe_sql(const ObSelectStmt &inner_stmt, ObSqlString &describe_sql);
  /* parse the generated sql and resolve it as the result of this statement */
  int parse_and_resolve_select_sql(const ObString &select_sql);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_DESCRIBE_SELECT_RESOLVER_ */

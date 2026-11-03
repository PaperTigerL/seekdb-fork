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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_FILE_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_FILE_RESOLVER_

#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
namespace sql
{

/**
 * @file ob_copy_to_file_resolver.h
 *
 * Resolver of `copy (select ...) to 'file' (options)`.
 *
 * The syntax is borrowed from duckdb, only the csv format is supported for now:
 *
 *   copy (select ...) to 'file' (format csv, header, nullstr 'NULL');
 *
 * The statement is implemented on top of the `select ... into outfile` mechanism:
 * the inner select statement is resolved as a normal select statement, and an
 * `ObSelectIntoItem` describing the target file and the csv options is attached
 * to it. So the statement is finally executed by the `ObSelectIntoOp`, exactly
 * like `select ... into outfile` does.
 */
class ObCopyToFileResolver : public ObSelectResolver
{
public:
  explicit ObCopyToFileResolver(ObResolverParams &params) : ObSelectResolver(params) {}
  virtual ~ObCopyToFileResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCopyToFileResolver);

  /* the options of the copy statement */
  struct CopyOptions
  {
    CopyOptions()
        : has_format_(false),
          format_(),
          write_header_(false),
          null_str_()
    {
    }
    bool has_format_;
    common::ObString format_;
    bool write_header_;
    common::ObString null_str_;
  };

  /* parse the option list, the options are linked by the T_LINK_NODE nodes */
  int parse_copy_options(const ParseNode *option_node, CopyOptions &options);
  /* parse one option, the option is a T_COPY_TO_OPTION node */
  int parse_copy_option(const ParseNode *option_node, CopyOptions &options);
  /* resolve the target file of the copy statement */
  int resolve_copy_file_name(const ParseNode *file_node, common::ObObj &file_name);
  /* collect the names of the output columns, they are written as the header line */
  int collect_column_names(const ObSelectStmt &select_stmt, common::ObIArray<common::ObString> &column_names);
  /* build the into item that describes the target file and the csv options */
  int build_into_item(const CopyOptions &options, const common::ObObj &file_name, ObSelectIntoItem *&into_item);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_COPY_TO_FILE_RESOLVER_ */

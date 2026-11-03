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
#include "sql/resolver/cmd/ob_copy_to_file_resolver.h"

#include "lib/string/ob_string.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

/* the supported copy options */
static const char *const COPY_OPTION_FORMAT = "format";
static const char *const COPY_OPTION_HEADER = "header";
static const char *const COPY_OPTION_NULLSTR = "nullstr";
/* the only format that is supported for now */
static const char *const COPY_FORMAT_CSV = "csv";
/* the separators of the csv file */
static const char *const COPY_CSV_FIELD_SEPARATOR = ",";
static const char *const COPY_CSV_LINE_SEPARATOR = "\n";

/* get the name of the output column of a select item */
static common::ObString get_copy_select_item_name(const SelectItem &item)
{
  common::ObString name;
  if (!item.alias_name_.empty()) {
    name = item.alias_name_;
  } else if (OB_NOT_NULL(item.expr_) && item.expr_->is_column_ref_expr()) {
    name = static_cast<ObColumnRefRawExpr *>(item.expr_)->get_column_name();
  } else if (OB_NOT_NULL(item.expr_)) {
    name = item.expr_name_;
  }
  return name;
}

int ObCopyToFileResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parse_tree.type_ != T_COPY_TO_FILE || parse_tree.num_child_ != 3
                  || OB_ISNULL(parse_tree.children_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree of copy to file", K(ret), K(parse_tree.type_));
  } else {
    CopyOptions options;
    common::ObObj file_name;
    /* the copy statement is executed as a select statement, the stmt type of the session is
     * used while resolving the expressions, so it must be fixed before the inner select
     * statement is resolved, otherwise the constants are resolved with a wrong stmt type */
    if (OB_NOT_NULL(session_info_)) {
      session_info_->set_stmt_type(stmt::T_SELECT);
      ObExecContext *exec_ctx = session_info_->get_cur_exec_ctx();
      if (OB_NOT_NULL(exec_ctx) && OB_NOT_NULL(exec_ctx->get_sql_ctx())) {
        exec_ctx->get_sql_ctx()->stmt_type_ = stmt::T_SELECT;
      }
    }
    if (OB_FAIL(parse_copy_options(parse_tree.children_[2], options))) {
      LOG_WARN("failed to parse the options of copy to file", K(ret));
    } else if (OB_FAIL(resolve_copy_file_name(parse_tree.children_[1], file_name))) {
      LOG_WARN("failed to resolve the target file of copy to file", K(ret));
    } else if (OB_UNLIKELY(OB_ISNULL(parse_tree.children_[0])
                           || parse_tree.children_[0]->type_ != T_SELECT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the inner statement of copy to file is not a select statement", K(ret));
    } else if (OB_FAIL(ObSelectResolver::resolve(*parse_tree.children_[0]))) {
      LOG_WARN("failed to resolve the inner select statement of copy to file", K(ret));
    } else {
      /* the inner select statement is the result of this statement, it carries the into item */
      ObSelectStmt *inner_stmt = get_select_stmt();
      ObSelectIntoItem *into_item = NULL;
      if (OB_ISNULL(inner_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the inner select statement of copy to file is not resolved", K(ret));
      } else if (OB_FAIL(build_into_item(options, file_name, into_item))) {
        LOG_WARN("failed to build the into item of copy to file", K(ret));
      } else if (OB_FAIL(collect_column_names(*inner_stmt, into_item->column_names_))) {
        LOG_WARN("failed to collect the column names of copy to file", K(ret));
      } else {
        inner_stmt->set_select_into(into_item);
      }
    }
  }
  return ret;
}

int ObCopyToFileResolver::parse_copy_options(const ParseNode *option_node, CopyOptions &options)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(option_node)) {
    /* no option is given, the default csv options are used */
  } else if (option_node->type_ == T_LINK_NODE) {
    /* the options are linked by the T_LINK_NODE nodes */
    if (OB_UNLIKELY(option_node->num_child_ != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid option list of copy to file", K(ret));
    } else if (OB_FAIL(parse_copy_options(option_node->children_[0], options))) {
    } else if (OB_FAIL(parse_copy_options(option_node->children_[1], options))) {
    } else { /* do nothing */ }
  } else if (OB_FAIL(parse_copy_option(option_node, options))) {
  } else { /* do nothing */ }
  return ret;
}

int ObCopyToFileResolver::parse_copy_option(const ParseNode *option_node, CopyOptions &options)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(option_node) || option_node->type_ != T_COPY_TO_OPTION
                  || option_node->num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid option of copy to file", K(ret));
  } else {
    const ParseNode *name_node = option_node->children_[0];
    const ParseNode *value_node = option_node->children_[1];
    if (OB_ISNULL(name_node)) {
      /* the format option, the value is the name of the format, e.g. `format csv` */
      if (OB_ISNULL(value_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the format option of copy to file has no value", K(ret));
      } else {
        common::ObString format(value_node->str_len_, value_node->str_value_);
        if (0 != format.case_compare(COPY_FORMAT_CSV)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the copy format");
        } else {
          options.has_format_ = true;
          options.format_ = format;
        }
      }
    } else {
      common::ObString name(name_node->str_len_, name_node->str_value_);
      if (0 == name.case_compare(COPY_OPTION_FORMAT)) {
        /* the format option given as a name and a value, e.g. `format 'csv'` */
        if (OB_ISNULL(value_node)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the copy format");
        } else {
          common::ObString format(value_node->str_len_, value_node->str_value_);
          if (0 != format.case_compare(COPY_FORMAT_CSV)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "the copy format");
          } else {
            options.has_format_ = true;
            options.format_ = format;
          }
        }
      } else if (0 == name.case_compare(COPY_OPTION_HEADER)) {
        /* the header option is a flag, it makes the column names be written first */
        if (OB_NOT_NULL(value_node)) {
          common::ObString value(value_node->str_len_, value_node->str_value_);
          options.write_header_ = !(0 == value.case_compare("false") || 0 == value.case_compare("0"));
        } else {
          options.write_header_ = true;
        }
      } else if (0 == name.case_compare(COPY_OPTION_NULLSTR)) {
        /* the nullstr option gives the text that represents a null value */
        if (OB_ISNULL(value_node)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the copy option nullstr");
        } else {
          options.null_str_ = common::ObString(value_node->str_len_, value_node->str_value_);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the copy option");
      }
    }
  }
  return ret;
}

int ObCopyToFileResolver::resolve_copy_file_name(const ParseNode *file_node, common::ObObj &file_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(file_node)
                  || (file_node->type_ != T_CHAR && file_node->type_ != T_VARCHAR
                      && !(T_QUESTIONMARK == file_node->type_
                           && OB_NOT_NULL(file_node->str_value_))))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the target file of copy");
  } else if (OB_FAIL(resolve_into_const_node(file_node, file_name))) {
    LOG_WARN("failed to resolve the target file of copy to file", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObCopyToFileResolver::collect_column_names(const ObSelectStmt &select_stmt,
                                                common::ObIArray<common::ObString> &column_names)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<SelectItem> &select_items = select_stmt.get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    const SelectItem &item = select_items.at(i);
    common::ObString name = get_copy_select_item_name(item);
    if (name.empty()) {
      /* the name of the expression is unknown, generate one for it */
      char name_buf[64];
      (void)snprintf(name_buf, sizeof(name_buf), "column_%ld", i + 1);
      name = common::ObString::make_string(name_buf);
    }
    if (OB_FAIL(column_names.push_back(name))) {
      LOG_WARN("failed to collect the column names of copy to file", K(ret));
    }
  }
  return ret;
}

int ObCopyToFileResolver::build_into_item(const CopyOptions &options,
                                          const common::ObObj &file_name,
                                          ObSelectIntoItem *&into_item)
{
  int ret = OB_SUCCESS;
  into_item = NULL;
  if (OB_UNLIKELY(OB_ISNULL(allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the allocator of the resolver is not set", K(ret));
  } else if (OB_ISNULL(into_item = static_cast<ObSelectIntoItem *>(
                         allocator_->alloc(sizeof(ObSelectIntoItem))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate the into item of copy to file", K(ret));
  } else {
    new(into_item) ObSelectIntoItem();
    into_item->into_type_ = T_INTO_OUTFILE;
    into_item->outfile_name_ = file_name;
    /* the csv format uses the comma as the field separator and the line feed as the line separator */
    into_item->field_str_.set_varchar(COPY_CSV_FIELD_SEPARATOR);
    into_item->field_str_.set_collation_type(ObCharset::get_system_collation());
    into_item->line_str_.set_varchar(COPY_CSV_LINE_SEPARATOR);
    into_item->line_str_.set_collation_type(ObCharset::get_system_collation());
    /* the fields are not enclosed, the csv file keeps the plain text of the values */
    into_item->closed_cht_.meta_.set_char();
    into_item->closed_cht_.set_char_value(NULL, 0);
    into_item->closed_cht_.set_collation_type(ObCharset::get_system_collation());
    into_item->is_optional_ = false;
    into_item->is_single_ = true;
    into_item->cs_type_ = ObCharset::get_system_collation();
    into_item->write_header_ = options.write_header_;
    if (!options.null_str_.empty()) {
      into_item->null_str_ = options.null_str_;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

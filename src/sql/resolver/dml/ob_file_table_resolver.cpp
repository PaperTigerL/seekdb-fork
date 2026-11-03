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
#include "sql/resolver/dml/ob_file_table_resolver.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/file_scan/ob_file_scan_reader.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{

using namespace common;

static const char *const FILE_SQL_ENABLE_SYS_VAR = "enable_file_sql";
static const char *const FILE_TABLE_GLOB_COLUMN_NAME = "file";

namespace
{

/// Whether the file name ends with the given suffix, ignoring the case.
bool ob_file_table_has_suffix(const ObString &path, const char *suffix)
{
  int64_t suffix_len = static_cast<int64_t>(strlen(suffix));
  if (path.length() < suffix_len) {
    return false;
  }
  // the tail is the last suffix_len characters of the path, the first argument
  // of the ObString constructor is the length of the tail
  ObString tail(suffix_len, path.ptr() + path.length() - suffix_len);
  return 0 == tail.case_compare(suffix);
}

/// The original text of a literal argument. The fast parser keeps the text on
/// the node even after the constant is turned into a question mark.
bool ob_file_table_literal_text(const ParseNode &node, ObString &text)
{
  bool found = false;
  if (OB_NOT_NULL(node.str_value_) && node.str_len_ > 0) {
    text.assign_ptr(node.str_value_, static_cast<int32_t>(node.str_len_));
    found = true;
  } else if (OB_NOT_NULL(node.raw_text_) && node.text_len_ > 0) {
    text.assign_ptr(node.raw_text_, static_cast<int32_t>(node.text_len_));
    found = true;
  }
  return found;
}

/// Read a boolean literal of a named argument.
int ob_file_table_parse_bool(const ParseNode &node, bool &val)
{
  int ret = OB_SUCCESS;
  if (T_BOOL == node.type_) {
    val = (0 != node.value_);
  } else if (T_QUESTIONMARK == node.type_) {
    ObString text;
    if (!ob_file_table_literal_text(node, text)) {
      ret = OB_NOT_SUPPORTED;
    } else if (0 == text.case_compare("true") || 0 == text.case_compare("1")) {
      val = true;
    } else if (0 == text.case_compare("false") || 0 == text.case_compare("0")) {
      val = false;
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  } else if (T_INT == node.type_) {
    val = (0 != node.value_);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

/// Read an integer literal of a named argument.
int ob_file_table_parse_int(const ParseNode &node, int64_t &val)
{
  int ret = OB_SUCCESS;
  if (T_INT == node.type_) {
    val = node.value_;
  } else if (T_UINT64 == node.type_) {
    val = static_cast<int64_t>(node.value_);
  } else if (T_QUESTIONMARK == node.type_) {
    ObString text;
    if (!ob_file_table_literal_text(node, text)) {
      ret = OB_NOT_SUPPORTED;
    } else {
      int64_t i = 0;
      bool neg = false;
      int64_t parsed = 0;
      if (text.length() > 0 && ('-' == text[0] || '+' == text[0])) {
        neg = ('-' == text[0]);
        i = 1;
      }
      if (i >= text.length()) {
        ret = OB_NOT_SUPPORTED;
      } else {
        for (; OB_SUCC(ret) && i < text.length(); ++i) {
          if (text[i] < '0' || text[i] > '9'
              || parsed > (INT64_MAX - 9) / 10) {
            ret = OB_NOT_SUPPORTED;
          } else {
            parsed = parsed * 10 + (text[i] - '0');
          }
        }
        if (OB_SUCC(ret)) {
          val = neg ? -parsed : parsed;
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

/// Whether the value of the format argument is accepted by the table function.
bool ob_file_table_format_accepted(ObFileTableFormat format, const ObString &format_value)
{
  bool accepted = false;
  switch (format) {
    case ObFileTableFormat::CSV:
      accepted = (0 == format_value.case_compare("csv"));
      break;
    case ObFileTableFormat::JSONL:
      accepted = (0 == format_value.case_compare("newline_delimited")
                  || 0 == format_value.case_compare("ndjson")
                  || 0 == format_value.case_compare("jsonl"));
      break;
    case ObFileTableFormat::PARQUET:
      accepted = (0 == format_value.case_compare("parquet"));
      break;
    default:
      // glob has no format
      break;
  }
  return accepted;
}

/// Whether the node carries a text literal, the file path has to be a literal.
bool ob_file_table_is_text_literal(const ParseNode &node)
{
  switch (node.type_) {
  case T_VARCHAR:
  case T_CHAR:
  case T_TEXT:
  case T_NCHAR:
  case T_CONCAT_STRING:
  // the parser turns the literals into placeholders, the text of the literal
  // is still carried by the node
  case T_QUESTIONMARK:
    return true;
  default:
    break;
  }
  return false;
}

} // namespace

int ObFileTableResolver::resolve_file_table_item(const ResolveContext &ctx,
                                                 const ParseNode &parse_tree,
                                                 TableItem *&tbl_item)
{
  int ret = OB_SUCCESS;
  const ParseNode *func_node = NULL;
  const ParseNode *arg_node = NULL;
  const ParseNode *alias_node = NULL;
  ObString func_name;
  ObString alias_name;
  ObString path;
  ObString format_value;
  bool has_header = true;
  int64_t sample_size = -1;
  ObFileTableFormat format = ObFileTableFormat::CSV;
  ObFileTableDef *table_def = NULL;
  TableItem *item = NULL;
  void *buf = NULL;

  if (OB_ISNULL(ctx.allocator_) || OB_ISNULL(ctx.session_info_) ||
      OB_ISNULL(ctx.params_) || OB_ISNULL(ctx.stmt_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(T_FILE_TABLE_EXPRESSION != parse_tree.type_ ||
                         3 != parse_tree.num_child_ ||
                         OB_ISNULL(parse_tree.children_))) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid file table expression",
             "node_type", parse_tree.type_, "num_child", parse_tree.num_child_);
  } else if (OB_ISNULL(func_node = parse_tree.children_[0]) ||
             OB_ISNULL(arg_node = parse_tree.children_[1])) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    func_name.assign_ptr(func_node->str_value_, func_node->str_len_);
    alias_node = parse_tree.children_[2];
    if (OB_NOT_NULL(alias_node)) {
      alias_name.assign_ptr(alias_node->str_value_, alias_node->str_len_);
    }
  }

  // the whole feature is guarded by a session variable, it has to be checked
  // before anything touches the file system
  OZ (check_file_sql_enabled(ctx.session_info_));
  // the name of the table function selects the format of the table
  OZ (dispatch_file_table_func(func_name, format));
  // the file path is the only positional argument, the others are named
  OZ (parse_file_table_args(*arg_node, format, path, has_header, sample_size, format_value));
  // the suffix of the file name has to match the format of the table function
  OZ (check_file_table_suffix(format, path));
  // the file has to be inside the directory allowed by secure_file_priv
  OZ (check_file_table_secure_path(ctx.session_info_, path));

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(item = ObDMLStmt::create_table_item(*ctx.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(buf = ctx.allocator_->alloc(sizeof(ObFileTableDef)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      table_def = new (buf) ObFileTableDef();
      item->table_id_ = ctx.table_id_;
      item->ref_id_ = common::OB_INVALID_ID;
      item->type_ = TableItem::FILE_TABLE;
      item->is_view_table_ = false;
      item->file_table_def_ = table_def;
      table_def->format_ = format;
      table_def->is_glob_ = (ObFileTableFormat::GLOB == format);
      table_def->has_header_ = has_header;
      table_def->sample_size_ = sample_size;
      // the table is named after the table function, the alias is optional
      OZ (ob_write_string(*ctx.allocator_, func_name, item->table_name_));
      if (OB_SUCC(ret) && !alias_name.empty()) {
        OZ (ob_write_string(*ctx.allocator_, alias_name, item->alias_name_));
      }
      if (OB_SUCC(ret)) {
        OZ (ob_write_string(*ctx.allocator_, path, table_def->path_));
      }
    }
  }

  // the columns of the table are inferred from the content of the file, the
  // file is only opened to read its schema and is closed right away
  if (OB_SUCC(ret)) {
    OZ (infer_file_table_columns(*ctx.allocator_, format, table_def->path_,
                                 has_header, sample_size, table_def->columns_));
  }

  if (OB_SUCC(ret)) {
    OZ (ctx.stmt_->add_table_item(ctx.session_info_, item));
  }

  // the columns have to be registered as column items of the statement, this
  // is what makes `select col ...` and `select *` able to resolve them
  if (OB_SUCC(ret)) {
    OZ (gen_file_table_column_items(ctx.stmt_, ctx.params_->expr_factory_, *item));
  }

  if (OB_SUCC(ret)) {
    tbl_item = item;
  }
  return ret;
}

int ObFileTableResolver::resolve_file_table_column_items(const TableItem &table_item,
                                                         const ObDMLStmt *stmt,
                                                         ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // the column items were registered in the order of the columns of the file
    OZ (stmt->get_column_items(table_item.table_id_, column_items));
  }
  return ret;
}

int ObFileTableResolver::check_file_table_column_exists(const TableItem &table_item,
                                                        const ObDMLStmt *stmt,
                                                        const ObString &col_name,
                                                        bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObSEArray<ObColumnRefRawExpr *, 4> column_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    OZ (stmt->get_column_exprs(table_item.table_id_, column_exprs));
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < column_exprs.count(); ++i) {
      if (OB_ISNULL(column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        is_exist = ObCharset::case_insensitive_equal(column_exprs.at(i)->get_column_name(),
                                                     col_name);
      }
    }
  }
  return ret;
}

int ObFileTableResolver::check_file_sql_enabled(ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  int64_t enable_file_sql = 0;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_sys_variable_by_name(
                       ObString(FILE_SQL_ENABLE_SYS_VAR), enable_file_sql))) {
    // an unknown variable means the feature is switched off
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
    LOG_WARN("failed to read the session variable enable_file_sql", K(ret));
  } else if (0 == enable_file_sql) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
    LOG_WARN("the session variable enable_file_sql is off", K(enable_file_sql));
  }
  return ret;
}

int ObFileTableResolver::dispatch_file_table_func(const ObString &func_name,
                                                  ObFileTableFormat &format)
{
  int ret = OB_SUCCESS;
  if (0 == func_name.case_compare("read_csv")) {
    format = ObFileTableFormat::CSV;
  } else if (0 == func_name.case_compare("read_json")) {
    format = ObFileTableFormat::JSONL;
  } else if (0 == func_name.case_compare("read_parquet")) {
    format = ObFileTableFormat::PARQUET;
  } else if (0 == func_name.case_compare("glob")) {
    format = ObFileTableFormat::GLOB;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
    LOG_WARN("unsupported file table function", K(func_name));
  }
  return ret;
}

int ObFileTableResolver::parse_file_table_args(const ParseNode &arg_node,
                                               ObFileTableFormat format,
                                               ObString &path,
                                               bool &has_header,
                                               int64_t &sample_size,
                                               ObString &format_value)
{
  int ret = OB_SUCCESS;
  const ParseNode *cur = &arg_node;
  const ParseNode *path_node = NULL;

  // the argument list is either the path alone or a link node whose first
  // child is the path and whose second child is the list of named arguments
  if (T_LINK_NODE == cur->type_) {
    if (OB_UNLIKELY(OB_ISNULL(cur->children_) || 2 != cur->num_child_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      path_node = cur->children_[0];
      cur = cur->children_[1];
    }
  } else {
    path_node = cur;
    cur = NULL;
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(path_node) || T_FILE_TABLE_ARG == path_node->type_) {
      // the file path is a mandatory positional argument
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function without a file path");
      LOG_WARN("the file table function has no file path");
    } else if (OB_UNLIKELY(!ob_file_table_is_text_literal(*path_node)
                           && OB_ISNULL(path_node->str_value_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function with a non literal file path");
      LOG_ERROR("the file path of a file table function is not a literal",
                "node_type", path_node->type_, "str_len", path_node->str_len_);
    } else {
      path.assign_ptr(path_node->str_value_, path_node->str_len_);
    }
  }

  // all the remaining arguments are named arguments
  while (OB_SUCC(ret) && OB_NOT_NULL(cur)) {
    const ParseNode *arg_node = NULL;
    if (T_LINK_NODE == cur->type_) {
      if (OB_UNLIKELY(OB_ISNULL(cur->children_) || 2 != cur->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      arg_node = cur->children_[0];
      cur = cur->children_[1];
    } else {
      arg_node = cur;
      cur = NULL;
    }
    OZ (parse_file_table_named_arg(*arg_node, format,
                                   has_header, sample_size, format_value));
  }
  return ret;
}

int ObFileTableResolver::parse_file_table_named_arg(const ParseNode &arg_node,
                                                    ObFileTableFormat format,
                                                    bool &has_header,
                                                    int64_t &sample_size,
                                                    ObString &format_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FILE_TABLE_ARG != arg_node.type_ ||
                  OB_ISNULL(arg_node.children_) || 2 != arg_node.num_child_ ||
                  OB_ISNULL(arg_node.children_[0]) || OB_ISNULL(arg_node.children_[1]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid named argument of a file table function", K(arg_node.type_));
  } else {
    const ParseNode *name_node = arg_node.children_[0];
    const ParseNode *value_node = arg_node.children_[1];
    ObString arg_name;
    arg_name.assign_ptr(name_node->str_value_, name_node->str_len_);
    if (0 == arg_name.case_compare("header")) {
      bool header = true;
      if (OB_FAIL(ob_file_table_parse_bool(*value_node, header))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the header argument of a file table function");
        LOG_WARN("the header argument is not a boolean", K(arg_name));
      } else {
        has_header = header;
      }
    } else if (0 == arg_name.case_compare("sample_size")) {
      int64_t sample = -1;
      if (OB_FAIL(ob_file_table_parse_int(*value_node, sample))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the sample_size argument of a file table function");
        LOG_WARN("the sample_size argument is not an integer", K(arg_name));
      } else if (OB_UNLIKELY(-1 != sample)) {
        // only a full scan is supported
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "a sample_size other than -1");
        LOG_WARN("only a full scan is supported", K(sample));
      } else {
        sample_size = sample;
      }
    } else if (0 == arg_name.case_compare("format")) {
      ObString format_text;
      if (OB_UNLIKELY(!ob_file_table_literal_text(*value_node, format_text))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the format argument of a file table function");
        LOG_WARN("the format argument is not a string", K(arg_name),
                 "node_type", value_node->type_);
      } else {
        format_value = format_text;
        if (OB_UNLIKELY(!ob_file_table_format_accepted(format, format_value))) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the format argument of a file table function");
          LOG_WARN("the format argument is not accepted by the file table function",
                   K(format_value), "format", static_cast<int32_t>(format));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "an unknown argument of a file table function");
      LOG_WARN("unknown argument of a file table function", K(arg_name));
    }
  }
  return ret;
}

int ObFileTableResolver::check_file_table_suffix(ObFileTableFormat format, const ObString &path)
{
  int ret = OB_SUCCESS;
  bool matched = true;
  if (ObFileTableFormat::CSV == format) {
    matched = ob_file_table_has_suffix(path, ".csv");
  } else if (ObFileTableFormat::JSONL == format) {
    matched = ob_file_table_has_suffix(path, ".jsonl")
              || ob_file_table_has_suffix(path, ".ndjson");
  } else if (ObFileTableFormat::PARQUET == format) {
    matched = ob_file_table_has_suffix(path, ".parquet");
  } else {
    // glob accepts any pattern
    matched = true;
  }
  if (OB_UNLIKELY(!matched)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "a file name whose suffix does not match the format");
    LOG_ERROR("the suffix of the file name does not match the format",
              "format", static_cast<int32_t>(format), "path", path);
  }
  return ret;
}

int ObFileTableResolver::check_file_table_secure_path(ObSQLSessionInfo *session_info,
                                                      const ObString &path)
{
  int ret = OB_SUCCESS;
  ObString secure_file_priv;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_secure_file_priv(secure_file_priv))) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("failed to read the session variable secure_file_priv", K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, path))) {
    // the file is outside of the directory allowed by secure_file_priv, the
    // error of check_secure_path is kept as it is
    LOG_WARN("the file is not allowed by secure_file_priv",
             K(ret), K(secure_file_priv), K(path));
  }
  return ret;
}

int ObFileTableResolver::infer_file_table_columns(ObIAllocator &allocator,
                                                  ObFileTableFormat format,
                                                  const ObString &path,
                                                  bool has_header,
                                                  int64_t sample_size,
                                                  ObSEArray<ObFileTableColumnDef, 16,
                                                            ModulePageAllocator, true> &columns)
{
  int ret = OB_SUCCESS;
  if (ObFileTableFormat::GLOB == format) {
    // a glob table always has one single column with the path of the file
    ObFileTableColumnDef col_def(ObString(FILE_TABLE_GLOB_COLUMN_NAME), ObVarcharType, false);
    OZ (columns.push_back(col_def));
  } else {
    ObFileScanFormat scan_format = ObFileScanFormat::CSV;
    if (ObFileTableFormat::JSONL == format) {
      scan_format = ObFileScanFormat::JSONL;
    } else if (ObFileTableFormat::PARQUET == format) {
      scan_format = ObFileScanFormat::PARQUET;
    }
    ObFileScanOptions options;
    options.format = scan_format;
    options.has_header = has_header;
    options.sample_size = sample_size;
    ObFileScanReader *reader = ObFileScanReaderFactory::create(allocator, scan_format);
    if (OB_ISNULL(reader)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (OB_FAIL(reader->open(allocator, path, options))) {
        LOG_WARN("failed to open the file of a file table", K(ret), K(path));
      } else {
        const ObSEArray<ObFileScanColumn, 16> &scan_columns = reader->columns();
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_columns.count(); ++i) {
          const ObFileScanColumn &scan_column = scan_columns.at(i);
          ObFileTableColumnDef col_def;
          col_def.type_ = scan_column.type;
          col_def.nullable_ = scan_column.nullable;
          // the name of the column belongs to the reader, copy it
          OZ (ob_write_string(allocator, scan_column.name, col_def.name_));
          if (OB_SUCC(ret)) {
            OZ (columns.push_back(col_def));
          }
        }
      }
      // the reader is only used to infer the schema of the file, the file is
      // not read here, so it is closed and released right away
      reader->close();
      reader->~ObFileScanReader();
      allocator.free(reader);
    }
  }
  return ret;
}

int ObFileTableResolver::gen_file_table_column_items(ObDMLStmt *stmt,
                                                    ObRawExprFactory *expr_factory,
                                                    TableItem &table_item)
{
  int ret = OB_SUCCESS;
  ObFileTableDef *table_def = table_item.file_table_def_;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr_factory) || OB_ISNULL(table_def)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObSEArray<ObFileTableColumnDef, 16, ModulePageAllocator, true> &columns
        = table_def->columns_;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const ObFileTableColumnDef &col_def = columns.at(i);
      ObColumnRefRawExpr *column_expr = NULL;
      OZ (expr_factory->create_raw_expr(T_REF_COLUMN, column_expr));
      if (OB_SUCC(ret) && OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_SUCC(ret)) {
        ObRawExprResType res_type;
        res_type.set_type(col_def.type_);
        if (ob_is_string_type(col_def.type_)) {
          res_type.set_collation_level(CS_LEVEL_IMPLICIT);
          res_type.set_default_collation_type();
        }
        column_expr->set_result_type(res_type);
        // a column that never holds a null is marked with the not null flag
        if (col_def.nullable_) {
          column_expr->unset_result_flag(NOT_NULL_FLAG);
        } else {
          column_expr->set_result_flag(NOT_NULL_FLAG);
        }
        column_expr->set_ref_id(table_item.table_id_, i + OB_APP_MIN_COLUMN_ID);
        column_expr->set_column_attr(table_item.get_object_name(), col_def.name_);
        if (OB_FAIL(column_expr->add_flag(IS_COLUMN))) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ColumnItem column_item;
          column_item.expr_ = column_expr;
          column_item.table_id_ = column_expr->get_table_id();
          column_item.column_id_ = column_expr->get_column_id();
          column_item.column_name_ = column_expr->get_column_name();
          OZ (stmt->add_column_item(column_item));
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

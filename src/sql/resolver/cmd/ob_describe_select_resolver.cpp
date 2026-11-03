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
#include "sql/resolver/cmd/ob_describe_select_resolver.h"

#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

/* the six columns of the describe result, they are fixed by duckdb */
static const char *DESCRIBE_COLUMN_NAME = "column_name";
static const char *DESCRIBE_COLUMN_TYPE = "column_type";
static const char *DESCRIBE_COLUMN_NULL = "null";
static const char *DESCRIBE_COLUMN_KEY = "key";
static const char *DESCRIBE_COLUMN_DEFAULT = "default";
static const char *DESCRIBE_COLUMN_EXTRA = "extra";
/* the hidden row number column, it is only used to keep the order of the rows */
static const char *DESCRIBE_ROW_NUMBER = "__rn";

/**
 * @brief map the ob type of a column to the type name reported by describe
 *
 * note that oceanbase has no dedicated boolean type, the boolean columns of
 * the file tables are stored as tinyint, so tinyint is reported as boolean.
 */
static const char *describe_column_type(ObObjType type)
{
  const char *type_name = "VARCHAR";
  switch (type) {
    case ObNullType:
      type_name = "NULL";
      break;
    case ObTinyIntType:
      type_name = "BOOLEAN";
      break;
    case ObSmallIntType:
      type_name = "SMALLINT";
      break;
    case ObIntType:
      type_name = "BIGINT";
      break;
    case ObFloatType:
      type_name = "FLOAT";
      break;
    case ObDoubleType:
      type_name = "DOUBLE";
      break;
    case ObNumberType:
      type_name = "DECIMAL";
      break;
    case ObVarcharType:
      type_name = "VARCHAR";
      break;
    case ObCharType:
      type_name = "CHAR";
      break;
    case ObTextType:
      type_name = "TEXT";
      break;
    case ObDateType:
      type_name = "DATE";
      break;
    case ObTimeType:
      type_name = "TIME";
      break;
    case ObDateTimeType:
      type_name = "DATETIME";
      break;
    case ObTimestampType:
      type_name = "TIMESTAMP";
      break;
    case ObJsonType:
      type_name = "JSON";
      break;
    default:
      break;
  }
  return type_name;
}

/* get the name of the output column of a select item */
static common::ObString get_select_item_name(const SelectItem &item)
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

/* append a quoted string literal, the single quote is escaped by doubling it */
static int append_quoted_text(ObSqlString &sql, const char *str, int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if ('\'' == str[i]) {
        ret = sql.append("''", 2);
      } else {
        ret = sql.append(str + i, 1);
      }
    }
    if (OB_SUCC(ret)) {
      ret = sql.append("'");
    }
  }
  return ret;
}

int ObDescribeSelectResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *select_node = NULL;
  if (OB_UNLIKELY(parse_tree.type_ != T_DESCRIBE_SELECT || parse_tree.num_child_ != 2
                  || OB_ISNULL(parse_tree.children_)
                  || OB_ISNULL(select_node = parse_tree.children_[0])
                  || select_node->type_ != T_SELECT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree of describe select", K(ret), K(parse_tree.type_));
  } else {
    ObSelectStmt *inner_stmt = NULL;
    if (OB_FAIL(resolve_inner_select(*select_node, inner_stmt))) {
      LOG_WARN("failed to resolve the select statement of describe select", K(ret));
    } else {
      ObSqlString describe_sql;
      if (OB_FAIL(gen_describe_sql(*inner_stmt, describe_sql))) {
        LOG_WARN("failed to generate the sql of describe select", K(ret));
      } else if (OB_FAIL(parse_and_resolve_select_sql(describe_sql.string()))) {
        LOG_WARN("failed to resolve the generated sql of describe select", K(ret));
      } else {
        /* the generated select statement is the result of this statement */
      }
    }
  }
  return ret;
}

int ObDescribeSelectResolver::resolve_inner_select(const ParseNode &select_node, ObSelectStmt *&inner_stmt)
{
  int ret = OB_SUCCESS;
  inner_stmt = NULL;
  ObSelectResolver inner_resolver(params_);
  if (OB_FAIL(inner_resolver.resolve(select_node))) {
    LOG_WARN("failed to resolve the inner select statement", K(ret));
  } else if (OB_ISNULL(inner_stmt = inner_resolver.get_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the inner select statement is not resolved", K(ret));
  } else {
    /* nothing to do */
  }
  return ret;
}

int ObDescribeSelectResolver::gen_describe_sql(const ObSelectStmt &inner_stmt, ObSqlString &describe_sql)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<SelectItem> &select_items = inner_stmt.get_select_items();
  if (OB_UNLIKELY(select_items.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the select statement of describe select has no output column", K(ret));
  } else if (OB_FAIL(describe_sql.append("select `column_name`,`column_type`,`null`,`key`,`default`,`extra` from ("))) {
    LOG_WARN("failed to build the sql of describe select", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      const SelectItem &item = select_items.at(i);
      if (OB_UNLIKELY(OB_ISNULL(item.expr_))) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      common::ObString name = get_select_item_name(item);
      const char *type_name = describe_column_type(item.expr_->get_result_type().get_type());
      /* a column that is known to be not null is reported as NO */
      const char *null_text = item.expr_->is_not_null_for_read() ? "NO" : "YES";
      char name_buf[64];
      const char *name_ptr = name.ptr();
      int64_t name_len = name.length();
      if (name_len <= 0) {
        (void)snprintf(name_buf, sizeof(name_buf), "column_%ld", i);
        name_ptr = name_buf;
        name_len = (int64_t)strlen(name_buf);
      }
      if (i > 0 && OB_FAIL(describe_sql.append(" union all "))) {
        break;
      }
      if (OB_FAIL(describe_sql.append_fmt("select %ld as `%s`,", i, DESCRIBE_ROW_NUMBER))) {
        break;
      }
      if (OB_FAIL(append_quoted_text(describe_sql, name_ptr, name_len))) {
        break;
      }
      if (OB_FAIL(describe_sql.append_fmt(" as `%s`,", DESCRIBE_COLUMN_NAME))) {
        break;
      }
      if (OB_FAIL(append_quoted_text(describe_sql, type_name, (int64_t)strlen(type_name)))) {
        break;
      }
      if (OB_FAIL(describe_sql.append_fmt(" as `%s`,", DESCRIBE_COLUMN_TYPE))) {
        break;
      }
      if (OB_FAIL(append_quoted_text(describe_sql, null_text, (int64_t)strlen(null_text)))) {
        break;
      }
      if (OB_FAIL(describe_sql.append_fmt(" as `%s`,'NULL' as `%s`,'NULL' as `%s`,'NULL' as `%s`",
                                          DESCRIBE_COLUMN_NULL, DESCRIBE_COLUMN_KEY,
                                          DESCRIBE_COLUMN_DEFAULT, DESCRIBE_COLUMN_EXTRA))) {
        break;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(describe_sql.append_fmt(") as `__t` order by `%s`", DESCRIBE_ROW_NUMBER))) {
      LOG_WARN("failed to build the sql of describe select", K(ret));
    }
  }
  return ret;
}

int ObDescribeSelectResolver::parse_and_resolve_select_sql(const ObString &select_sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the session info or the allocator is not set", K(ret));
  } else {
    ParseResult select_result;
    ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
    if (OB_FAIL(parser.parse(select_sql, select_result))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parse the generated sql of describe select", K(ret), K(select_sql));
    } else if (OB_UNLIKELY(OB_ISNULL(select_result.result_tree_) || select_result.result_tree_->num_child_ != 1
                           || OB_ISNULL(select_result.result_tree_->children_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the generated sql of describe select is not parsed", K(ret));
    } else if (OB_UNLIKELY(OB_ISNULL(select_result.result_tree_->children_[0]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the generated sql of describe select is not parsed", K(ret));
    } else {
      ParseNode *select_stmt_node = select_result.result_tree_->children_[0];
      ret = ObSelectResolver::resolve(*select_stmt_node);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to resolve the generated sql of describe select", K(ret), K(select_sql));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

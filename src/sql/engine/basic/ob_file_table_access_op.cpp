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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_table_access_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <algorithm>
#include <string>
#include <vector>

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER_INHERIT(ObFileTableAccessSpec,
                            ObOpSpec,
                            column_exprs_,
                            format_,
                            has_header_,
                            sample_size_,
                            row_cnt_,
                            is_glob_,
                            path_);

namespace
{
// 文本解析用的栈缓冲，文件里的单个字段不会很长。
const int64_t OB_FILE_TABLE_MAX_TEXT_LEN = 1024;

// 通配符匹配：支持 '*' 与 '?'。
bool ob_file_table_glob_match(const char *pattern,
                              int64_t pattern_len,
                              const char *name,
                              int64_t name_len)
{
  int64_t p = 0;
  int64_t n = 0;
  int64_t star_pos = -1;
  int64_t star_match = 0;
  while (n < name_len) {
    if (p < pattern_len && ('?' == pattern[p] || pattern[p] == name[n])) {
      ++p;
      ++n;
    } else if (p < pattern_len && '*' == pattern[p]) {
      star_pos = p++;
      star_match = n;
    } else if (star_pos >= 0) {
      p = star_pos + 1;
      n = ++star_match;
    } else {
      return false;
    }
  }
  while (p < pattern_len && '*' == pattern[p]) {
    ++p;
  }
  return p == pattern_len;
}

bool ob_file_table_is_bool_text(const char *data, int64_t len, bool &value)
{
  value = false;
  if (4 == len) {
    if (('t' == data[0] || 'T' == data[0]) && 'r' == data[1] && 'u' == data[2] && 'e' == data[3]) {
      value = true;
      return true;
    }
  } else if (5 == len) {
    if (('f' == data[0] || 'F' == data[0]) && 'a' == data[1] && 'l' == data[2] && 's' == data[3]
        && 'e' == data[4]) {
      value = false;
      return true;
    }
  }
  return false;
}

// 把文本解析成整数；解析失败返回 OB_ERR_UNEXPECTED。
int ob_file_table_parse_int(const ObString &text, int64_t &value)
{
  int ret = OB_SUCCESS;
  char buf[OB_FILE_TABLE_MAX_TEXT_LEN];
  if (text.length() <= 0 || text.length() >= static_cast<int64_t>(sizeof(buf))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    char *end = NULL;
    errno = 0;
    const long long parsed = strtoll(buf, &end, 10);
    if (end == buf || '\0' != *end || 0 != errno) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = static_cast<int64_t>(parsed);
    }
  }
  return ret;
}

// 把文本解析成浮点数；解析失败返回 OB_ERR_UNEXPECTED。
int ob_file_table_parse_double(const ObString &text, double &value)
{
  int ret = OB_SUCCESS;
  char buf[OB_FILE_TABLE_MAX_TEXT_LEN];
  if (text.length() <= 0 || text.length() >= static_cast<int64_t>(sizeof(buf))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    char *end = NULL;
    errno = 0;
    const double parsed = strtod(buf, &end);
    if (end == buf || '\0' != *end || 0 != errno) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = parsed;
    }
  }
  return ret;
}

} // namespace

ObFileTableAccessOp::ObFileTableAccessOp(ObExecContext &exec_ctx,
                                         const ObOpSpec &spec,
                                         ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    reader_(NULL),
    schema_alloc_(),
    row_alloc_(),
    row_(),
    glob_files_(),
    glob_idx_(0) {}

int ObFileTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx_.get_my_session();
  int64_t enable_file_sql = 0;
  row_alloc_.reset();
  glob_files_.reset();
  glob_idx_ = 0;
  if (OB_UNLIKELY(MY_SPEC.column_exprs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_sys_variable_by_name(
                       ObString("enable_file_sql"), enable_file_sql))) {
    // the plan may be cached, so the switch is checked again for every execution
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
    LOG_WARN("failed to read the session variable enable_file_sql", K(ret));
  } else if (0 == enable_file_sql) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
    LOG_WARN("the session variable enable_file_sql is off", K(enable_file_sql));
  } else if (OB_FAIL(open_data_source())) {
  } else { /* do nothing */ }
  return ret;
}

int ObFileTableAccessOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (OB_FAIL(close_data_source())) {
  } else if (OB_FAIL(open_data_source())) {
  } else { /* do nothing */ }
  return ret;
}

int ObFileTableAccessOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
  } else {
    glob_idx_ = 0;
  }
  return ret;
}

int ObFileTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
  } else {
    clear_evaluated_flag();
    if (MY_SPEC.is_glob_) {
      ret = calc_glob_row();
    } else {
      ret = calc_next_row();
    }
    if (OB_ITER_END == ret) {
      ret = OB_ITER_END;
    } else if (OB_SUCC(ret)) {
      const int64_t col_num = MY_SPEC.column_exprs_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
        ObExpr *output = MY_SPEC.output_.at(i);
        if (i < col_num && output == MY_SPEC.column_exprs_.at(i)) {
          /* need do nothing */
        } else {
          ObDatum *dst_datum = NULL;
          if (OB_FAIL(output->eval(eval_ctx_, dst_datum))) {
          }
        }
      }
    }
  }
  return ret;
}

int ObFileTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  glob_idx_ = 0;
  glob_files_.reset();
  if (OB_FAIL(close_data_source())) {
  } else {
    row_alloc_.reset();
    schema_alloc_.reset();
  }
  return ret;
}

int ObFileTableAccessOp::open_data_source()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_glob_) {
    ret = expand_glob_files();
  } else {
    ObFileScanFormat format = ObFileScanFormat::CSV;
    switch (MY_SPEC.format_) {
      case static_cast<int32_t>(ObFileTableFormat::CSV):
        format = ObFileScanFormat::CSV;
        break;
      case static_cast<int32_t>(ObFileTableFormat::JSONL):
        format = ObFileScanFormat::JSONL;
        break;
      case static_cast<int32_t>(ObFileTableFormat::PARQUET):
        format = ObFileScanFormat::PARQUET;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    if (OB_SUCC(ret)) {
      ObFileScanOptions options;
      options.format = format;
      options.has_header = MY_SPEC.has_header_;
      options.sample_size = MY_SPEC.sample_size_;
      reader_ = ObFileScanReaderFactory::create(schema_alloc_, format);
      if (OB_ISNULL(reader_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(reader_->open(schema_alloc_, MY_SPEC.path_, options))) {
        LOG_WARN("failed to open file table data source",
                 K(ret), K(MY_SPEC.path_), K(format));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObFileTableAccessOp::close_data_source()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    /* do nothing */
  } else {
    reader_->close();
    reader_->~ObFileScanReader();
    schema_alloc_.free(reader_);
    reader_ = NULL;
  }
  return ret;
}

int ObFileTableAccessOp::expand_glob_files()
{
  int ret = OB_SUCCESS;
  const ObString &pattern = MY_SPEC.path_;
  if (pattern.length() <= 0 || OB_ISNULL(pattern.ptr())) {
    return OB_INVALID_ARGUMENT;
  }
  const char *pattern_ptr = pattern.ptr();
  const int64_t pattern_len = pattern.length();
  int64_t slash_pos = -1;
  for (int64_t i = pattern_len - 1; i >= 0; --i) {
    if ('/' == pattern_ptr[i]) {
      slash_pos = i;
      break;
    }
  }
  const char *dir_ptr = ".";
  int64_t dir_len = 1;
  const char *name_ptr = pattern_ptr;
  int64_t name_pattern_len = pattern_len;
  if (slash_pos >= 0) {
    dir_ptr = pattern_ptr;
    dir_len = (0 == slash_pos ? 1 : slash_pos);
    name_ptr = pattern_ptr + slash_pos + 1;
    name_pattern_len = pattern_len - slash_pos - 1;
  }
  std::vector<std::string> paths;
  DIR *dir = NULL;
  /* the directory part of the pattern has to be a null terminated string */
  std::string dir_name(dir_ptr, dir_len);
  if (OB_ISNULL(dir = ::opendir(dir_name.c_str()))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("failed to open glob directory", K(ret), K(dir_name.c_str()));
  } else {
    struct dirent *entry = NULL;
    while (OB_SUCC(ret) && NULL != (entry = ::readdir(dir))) {
      const char *name = entry->d_name;
      if ('.' == name[0] && ('\0' == name[1] || ('.' == name[1] && '\0' == name[2]))) {
        continue;
      }
      const int64_t name_len = static_cast<int64_t>(strlen(name));
      if (!ob_file_table_glob_match(name_ptr, name_pattern_len, name, name_len)) {
        continue;
      }
      std::string full_path(dir_ptr, dir_len);
      if (!full_path.empty() && '/' != full_path[full_path.length() - 1]) {
        full_path.push_back('/');
      }
      full_path.append(name, name_len);
      paths.push_back(full_path);
    }
    ::closedir(dir);
  }
  if (OB_SUCC(ret)) {
    // 结果按文件名升序；同一目录下的完整路径排序等价于文件名排序。
    std::sort(paths.begin(), paths.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(paths.size()); ++i) {
      const std::string &path = paths.at(i);
      char *buf = static_cast<char *>(schema_alloc_.alloc(path.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, path.c_str(), path.length());
        if (OB_FAIL(glob_files_.push_back(ObString(static_cast<int32_t>(path.length()), buf)))) {
        }
      }
    }
  }
  return ret;
}

int ObFileTableAccessOp::calc_next_row()
{
  int ret = OB_SUCCESS;
  const int64_t col_num = MY_SPEC.column_exprs_.count();
  if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // 逐行回收上一行的文本缓冲，保证不会把整个文件物化到内存。
    row_alloc_.reset();
    if (OB_FAIL(reader_->next_row(row_alloc_, row_))) {
      /* OB_ITER_END 直接向上传递 */
    } else if (OB_UNLIKELY(row_.count() != col_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file table column count mismatch", K(ret), K(row_.count()), K(col_num));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_num; ++i) {
        ObExpr *dst_expr = MY_SPEC.column_exprs_.at(i);
        const ObFileScanValue &value = row_.at(i);
        if (value.is_null) {
          ret = set_null_datum(dst_expr);
        } else {
          ret = set_datum_from_text(dst_expr, value.text, dst_expr->datum_meta_.type_);
        }
      }
    }
  }
  return ret;
}

int ObFileTableAccessOp::calc_glob_row()
{
  int ret = OB_SUCCESS;
  if (glob_idx_ >= glob_files_.count()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(MY_SPEC.column_exprs_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExpr *dst_expr = MY_SPEC.column_exprs_.at(0);
    if (OB_FAIL(set_datum_from_text(dst_expr, glob_files_.at(glob_idx_),
                                   dst_expr->datum_meta_.type_))) {
    } else {
      ++glob_idx_;
    }
  }
  return ret;
}

int ObFileTableAccessOp::set_null_datum(ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  dst_datum.set_null();
  dst_expr->set_evaluated_projected(eval_ctx_);
  return ret;
}

int ObFileTableAccessOp::set_datum_from_text(ObExpr *dst_expr,
                                              const ObString &text,
                                              ObObjType type)
{
  int ret = OB_SUCCESS;
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  switch (type) {
    case ObTinyIntType: {
      // BOOLEAN：true/false -> 1/0，其余按整数解析。
      bool bool_value = false;
      int64_t int_value = 0;
      if (ob_file_table_is_bool_text(text.ptr(), text.length(), bool_value)) {
        dst_datum.set_int(bool_value ? 1 : 0);
      } else if (OB_FAIL(ob_file_table_parse_int(text, int_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse boolean text", K(ret), K(text));
      } else {
        dst_datum.set_int(int_value);
      }
      break;
    }
    case ObIntType:
    case ObInt32Type:
    case ObSmallIntType:
    case ObMediumIntType: {
      int64_t int_value = 0;
      if (OB_FAIL(ob_file_table_parse_int(text, int_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse integer text", K(ret), K(text));
      } else {
        dst_datum.set_int(int_value);
      }
      break;
    }
    case ObDoubleType:
    case ObFloatType: {
      double double_value = 0.0;
      if (OB_FAIL(ob_file_table_parse_double(text, double_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse double text", K(ret), K(text));
      } else {
        dst_datum.set_double(double_value);
      }
      break;
    }
    case ObDateType: {
      int32_t date_value = 0;
      if (OB_FAIL(ObTimeConverter::str_to_date(text, date_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse date text", K(ret), K(text));
      } else {
        dst_datum.set_date(date_value);
      }
      break;
    }
    case ObDateTimeType: {
      // 同时支持 "2026-08-01 10:11:12" 与 "2026-08-03T12:13:14" 两种写法。
      char buf[OB_FILE_TABLE_MAX_TEXT_LEN];
      ObString normalized;
      if (text.length() > 10 && ('T' == text.ptr()[10] || 't' == text.ptr()[10])
          && text.length() < static_cast<int64_t>(sizeof(buf))) {
        MEMCPY(buf, text.ptr(), text.length());
        buf[10] = ' ';
        normalized.assign(buf, text.length());
      } else {
        normalized = text;
      }
      int64_t datetime_value = 0;
      ObTimeConvertCtx cvrt_ctx(NULL, false);
      if (OB_FAIL(ObTimeConverter::str_to_datetime(normalized, cvrt_ctx, datetime_value, NULL, 0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to parse datetime text", K(ret), K(text));
      } else {
        dst_datum.set_datetime(datetime_value);
      }
      break;
    }
    default: {
      // 字符串列：复制到表达式的结果缓冲，避免指向逐行回收的内存。
      ObDatum src_datum;
      src_datum.set_string(text.ptr(), text.length());
      if (OB_FAIL(dst_expr->deep_copy_datum(eval_ctx_, src_datum))) {
        LOG_WARN("failed to copy string datum", K(ret), K(text));
      }
      break;
    }
  }
  if (OB_SUCC(ret)) {
    dst_expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

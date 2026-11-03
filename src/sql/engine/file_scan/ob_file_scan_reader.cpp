/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 文件即数据库：纯文件解析层。
 *
 * 该文件实现三种格式的行流式读取与 schema/类型推断：
 *   - CSV   : RFC4180 引号规则，支持字段内换行、"" 转义、CRLF/BOM；未加引号的空字段为 NULL。
 *   - JSONL : 每行一个 JSON 对象，取各行 key 的并集作为 schema，缺字段为 NULL，不支持嵌套对象/数组。
 *   - Parquet: 自带 Thrift compact 协议解析器，支持 PLAIN / 字典编码、UNCOMPRESSED / SNAPPY、
 *              DATA_PAGE / DATA_PAGE_V2。
 *
 * 设计约定：
 *   1. ObFileScanValue 只承载原始文本与是否为 NULL，不做类型转换，类型转换由上层按 columns() 完成。
 *   2. open() 会完整扫描一遍文件用于推断 schema（不物化整个文件），随后 next_row() 重新打开文件流式产出。
 *   3. 类型推断优先级：BOOLEAN(true/false) -> DATE(YYYY-MM-DD) -> DATETIME(空格/T 分隔)
 *      -> BIGINT -> DOUBLE -> VARCHAR；同列归一：整型+小数=>DOUBLE，DATE+DATETIME=>DATETIME，其余冲突=>VARCHAR。
 */

#include "sql/engine/file_scan/ob_file_scan_reader.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <new>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <map>
#include <string>
#include <vector>

#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{

using common::ObIAllocator;
using common::ObObjType;
using common::ObString;

namespace
{

//============================= 常量 =============================
const int64_t OB_FILE_SCAN_IO_BUF_SIZE = 64 * 1024;
const int64_t OB_FILE_SCAN_MAX_RECORD_SIZE = 256 * 1024 * 1024;
const int64_t OB_FILE_SCAN_MAX_COLUMNS = 4096;
const int64_t OB_FILE_SCAN_MAX_PAGE_HEADER_SIZE = 1024 * 1024;

//============================= 文本工具 =============================

inline bool ob_scan_is_space(char c)
{
  return ' ' == c || '\t' == c || '\r' == c || '\n' == c;
}

inline bool ob_scan_is_digit(char c)
{
  return c >= '0' && c <= '9';
}

inline bool ob_scan_all_digits(const char *data, int64_t beg, int64_t len)
{
  for (int64_t i = 0; i < len; ++i) {
    if (!ob_scan_is_digit(data[beg + i])) {
      return false;
    }
  }
  return true;
}

inline int ob_scan_to_int(const char *data, int64_t beg, int64_t len)
{
  int value = 0;
  for (int64_t i = 0; i < len; ++i) {
    value = value * 10 + (data[beg + i] - '0');
  }
  return value;
}

// 将文本拷贝到调用方分配器，返回可直接持有的 ObString。
int ob_scan_copy_to_allocator(common::ObIAllocator &allocator, const char *data, int64_t len,
                              common::ObString &out)
{
  int ret = OB_SUCCESS;
  out = common::ObString();
  if (len <= 0) {
    return ret;
  }
  char *buf = static_cast<char *>(allocator.alloc(len));
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    return ret;
  }
  memcpy(buf, data, len);
  out = common::ObString(len, buf);
  return ret;
}

// 清空一行结果，复用底层数组，避免反复申请内存。
inline void ob_scan_reset_row(common::ObSEArray<ObFileScanValue, 16> &row)
{
  while (row.count() > 0) {
    row.pop_back();
  }
}

//============================= 类型推断 =============================

enum ObScanKind : uint8_t {
  SCAN_KIND_UNKNOWN = 0,
  SCAN_KIND_BOOLEAN,
  SCAN_KIND_DATE,
  SCAN_KIND_DATETIME,
  SCAN_KIND_BIGINT,
  SCAN_KIND_DOUBLE,
  SCAN_KIND_VARCHAR,
};

inline ObScanKind ob_scan_merge_kind(ObScanKind left, ObScanKind right)
{
  if (left == right) {
    return left;
  }
  if (SCAN_KIND_UNKNOWN == left) {
    return right;
  }
  if (SCAN_KIND_UNKNOWN == right) {
    return left;
  }
  if ((SCAN_KIND_BIGINT == left && SCAN_KIND_DOUBLE == right)
      || (SCAN_KIND_DOUBLE == left && SCAN_KIND_BIGINT == right)) {
    return SCAN_KIND_DOUBLE;
  }
  if ((SCAN_KIND_DATE == left && SCAN_KIND_DATETIME == right)
      || (SCAN_KIND_DATETIME == left && SCAN_KIND_DATE == right)) {
    return SCAN_KIND_DATETIME;
  }
  return SCAN_KIND_VARCHAR;
}

inline ObObjType ob_scan_kind_to_type(ObScanKind kind)
{
  switch (kind) {
    case SCAN_KIND_BOOLEAN:
      return common::ObTinyIntType;    // mysql BOOLEAN == TINYINT(1)
    case SCAN_KIND_DATE:
      return common::ObDateType;
    case SCAN_KIND_DATETIME:
      return common::ObDateTimeType;
    case SCAN_KIND_BIGINT:
      return common::ObIntType;
    case SCAN_KIND_DOUBLE:
      return common::ObDoubleType;
    default:
      return common::ObVarcharType;
  }
}

inline bool ob_scan_is_bool_text(const char *data, int64_t len)
{
  if (4 == len) {
    return ('t' == data[0] || 'T' == data[0]) && 'r' == data[1] && 'u' == data[2] && 'e' == data[3];
  } else if (5 == len) {
    return ('f' == data[0] || 'F' == data[0]) && 'a' == data[1] && 'l' == data[2] && 's' == data[3]
           && 'e' == data[4];
  }
  return false;
}

inline bool ob_scan_is_date_text(const char *data, int64_t len)
{
  if (10 != len) {
    return false;
  }
  if (!ob_scan_all_digits(data, 0, 4) || '-' != data[4] || !ob_scan_all_digits(data, 5, 2)
      || '-' != data[7] || !ob_scan_all_digits(data, 8, 2)) {
    return false;
  }
  const int month = ob_scan_to_int(data, 5, 2);
  const int day = ob_scan_to_int(data, 8, 2);
  return month >= 1 && month <= 12 && day >= 1 && day <= 31;
}

inline bool ob_scan_is_datetime_text(const char *data, int64_t len)
{
  if (len < 19) {
    return false;
  }
  if (!ob_scan_is_date_text(data, 10)) {
    return false;
  }
  if (' ' != data[10] && 'T' != data[10] && 't' != data[10]) {
    return false;
  }
  if (!ob_scan_all_digits(data, 11, 2) || ':' != data[13] || !ob_scan_all_digits(data, 14, 2)
      || ':' != data[16] || !ob_scan_all_digits(data, 17, 2)) {
    return false;
  }
  const int hour = ob_scan_to_int(data, 11, 2);
  const int minute = ob_scan_to_int(data, 14, 2);
  const int second = ob_scan_to_int(data, 17, 2);
  if (hour > 23 || minute > 59 || second > 60) {
    return false;
  }
  if (19 == len) {
    return true;
  }
  if ('.' != data[19] || len <= 20) {
    return false;
  }
  return ob_scan_all_digits(data, 20, len - 20);
}

inline bool ob_scan_is_int_text(const char *data, int64_t len)
{
  int64_t beg = 0;
  bool negative = false;
  if (len > 0 && ('+' == data[0] || '-' == data[0])) {
    negative = ('-' == data[0]);
    beg = 1;
  }
  if (beg >= len) {
    return false;
  }
  int64_t sig_beg = beg;
  while (sig_beg < len && '0' == data[sig_beg]) {
    ++sig_beg;
  }
  if (len - sig_beg > 19) {
    return false;
  }
  unsigned long long value = 0;
  for (int64_t i = beg; i < len; ++i) {
    if (!ob_scan_is_digit(data[i])) {
      return false;
    }
    value = value * 10ULL + static_cast<unsigned long long>(data[i] - '0');
  }
  if (negative) {
    return value <= 9223372036854775808ULL;
  }
  return value <= 9223372036854775807ULL;
}

inline bool ob_scan_is_double_text(const char *data, int64_t len)
{
  int64_t pos = 0;
  if (pos < len && ('+' == data[pos] || '-' == data[pos])) {
    ++pos;
  }
  int64_t int_digits = 0;
  while (pos < len && ob_scan_is_digit(data[pos])) {
    ++pos;
    ++int_digits;
  }
  int64_t frac_digits = 0;
  if (pos < len && '.' == data[pos]) {
    ++pos;
    while (pos < len && ob_scan_is_digit(data[pos])) {
      ++pos;
      ++frac_digits;
    }
  }
  if (0 == int_digits && 0 == frac_digits) {
    return false;
  }
  if (pos < len && ('e' == data[pos] || 'E' == data[pos])) {
    ++pos;
    if (pos < len && ('+' == data[pos] || '-' == data[pos])) {
      ++pos;
    }
    int64_t exp_digits = 0;
    while (pos < len && ob_scan_is_digit(data[pos])) {
      ++pos;
      ++exp_digits;
    }
    if (0 == exp_digits) {
      return false;
    }
  }
  return pos == len;
}

// 依据文本形态推断列类型；不做 trim，类型与原始文本保持一致。
inline ObScanKind ob_scan_detect_kind(const char *data, int64_t len)
{
  if (len <= 0) {
    return SCAN_KIND_UNKNOWN;
  }
  if (ob_scan_is_bool_text(data, len)) {
    return SCAN_KIND_BOOLEAN;
  }
  if (ob_scan_is_date_text(data, len)) {
    return SCAN_KIND_DATE;
  }
  if (ob_scan_is_datetime_text(data, len)) {
    return SCAN_KIND_DATETIME;
  }
  if (ob_scan_is_int_text(data, len)) {
    return SCAN_KIND_BIGINT;
  }
  if (ob_scan_is_double_text(data, len)) {
    return SCAN_KIND_DOUBLE;
  }
  return SCAN_KIND_VARCHAR;
}

//============================= 文件读取基础设施 =============================

class ObFileScanFile
{
public:
  ObFileScanFile() : fd_(-1), file_size_(0) {}
  ~ObFileScanFile() { close(); }

  int open(const char *path)
  {
    int ret = OB_SUCCESS;
    close();
    if (OB_ISNULL(path)) {
      return common::OB_INVALID_ARGUMENT;
    }
    fd_ = ::open(path, O_RDONLY);
    if (fd_ < 0) {
      return (ENOENT == errno) ? common::OB_FILE_NOT_EXIST : common::OB_IO_ERROR;
    }
    struct stat file_stat;
    if (0 != ::fstat(fd_, &file_stat)) {
      close();
      return common::OB_IO_ERROR;
    }
    file_size_ = file_stat.st_size;
    return ret;
  }

  int close()
  {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    file_size_ = 0;
    return OB_SUCCESS;
  }

  bool is_opened() const { return fd_ >= 0; }
  int64_t size() const { return file_size_; }

  int read(char *buf, int64_t size, int64_t &read_len)
  {
    int ret = OB_SUCCESS;
    read_len = 0;
    if (fd_ < 0) {
      return common::OB_NOT_INIT;
    }
    while (read_len < size) {
      const ssize_t n = ::read(fd_, buf + read_len, size - read_len);
      if (n < 0) {
        if (EINTR == errno) {
          continue;
        }
        ret = common::OB_IO_ERROR;
        break;
      }
      if (0 == n) {
        break;
      }
      read_len += n;
    }
    return ret;
  }

  int pread(char *buf, int64_t size, int64_t offset, int64_t &read_len)
  {
    int ret = OB_SUCCESS;
    read_len = 0;
    if (fd_ < 0) {
      return common::OB_NOT_INIT;
    }
    while (read_len < size) {
      const ssize_t n = ::pread(fd_, buf + read_len, size - read_len, offset + read_len);
      if (n < 0) {
        if (EINTR == errno) {
          continue;
        }
        ret = common::OB_IO_ERROR;
        break;
      }
      if (0 == n) {
        break;
      }
      read_len += n;
    }
    return ret;
  }

private:
  int fd_;
  int64_t file_size_;
};

// 分块读取器，供 CSV / JSONL 记录扫描使用。
class ObChunkReader
{
public:
  ObChunkReader() : pos_(0), end_(0), eof_(false) {}
  ~ObChunkReader() { close(); }

  int open(const char *path)
  {
    int ret = OB_SUCCESS;
    buf_.resize(OB_FILE_SCAN_IO_BUF_SIZE);
    pos_ = 0;
    end_ = 0;
    eof_ = false;
    if (OB_ISNULL(path)) {
      return common::OB_INVALID_ARGUMENT;
    }
    file_path_ = path;
    if (OB_FAIL(file_.open(path))) {
      ret = common::OB_IO_ERROR;
    }
    return ret;
  }

  int reset()
  {
    int ret = OB_SUCCESS;
    pos_ = 0;
    end_ = 0;
    eof_ = false;
    if (OB_FAIL(file_.open(file_path_.c_str()))) {
      ret = common::OB_IO_ERROR;
    }
    return ret;
  }

  void close()
  {
    file_.close();
    pos_ = 0;
    end_ = 0;
    eof_ = false;
  }

  int fill()
  {
    int ret = OB_SUCCESS;
    int64_t read_len = 0;
    if (OB_FAIL(file_.read(buf_.data(), static_cast<int64_t>(buf_.size()), read_len))) {
      ret = common::OB_IO_ERROR;
    } else {
      pos_ = 0;
      end_ = read_len;
      if (0 == read_len) {
        eof_ = true;
      }
    }
    return ret;
  }

  char *buf() { return buf_.data(); }
  int64_t pos() const { return pos_; }
  int64_t end() const { return end_; }
  void set_pos(int64_t pos) { pos_ = pos; }
  bool eof() const { return eof_; }

private:
  ObFileScanFile file_;
  std::string file_path_;
  std::vector<char> buf_;
  int64_t pos_;
  int64_t end_;
  bool eof_;
};

//============================= CSV =============================

struct ObCsvField {
  int64_t offset;
  int64_t length;
  bool quoted;
};

// 记录扫描器：按 RFC4180 识别记录边界（引号内的换行不算记录结束）。
class ObCsvRecordScanner
{
public:
  ObCsvRecordScanner() : in_quote_(false), quote_pending_(false), bom_checked_(false) {}

  int open(const char *path)
  {
    int ret = OB_SUCCESS;
    in_quote_ = false;
    quote_pending_ = false;
    bom_checked_ = false;
    record_.clear();
    if (OB_FAIL(reader_.open(path))) {
      ret = common::OB_IO_ERROR;
    }
    return ret;
  }

  // 读取下一条记录，record 不含行尾换行符；is_eof 为 true 表示已无更多记录。
  int next(std::vector<char> &record, bool &is_eof)
  {
    int ret = OB_SUCCESS;
    is_eof = false;
    while (true) {
      if (reader_.pos() < reader_.end()) {
        int64_t scan = reader_.pos();
        bool in_quote = in_quote_;
        bool quote_pending = quote_pending_;
        int64_t record_end = -1;
        while (scan < reader_.end()) {
          const char c = reader_.buf()[scan];
          if (quote_pending) {
            quote_pending = false;
            if ('"' == c) {
              ++scan;
              continue;
            }
            in_quote = false;
            if ('\n' == c) {
              record_end = scan + 1;
              break;
            }
            ++scan;
            continue;
          }
          if (in_quote) {
            if ('"' == c) {
              quote_pending = true;
            }
            ++scan;
            continue;
          }
          if ('"' == c) {
            in_quote = true;  // 进入引号字段，其后的换行不再结束记录
            ++scan;
            continue;
          }
          if ('\n' == c) {
            record_end = scan + 1;
            break;
          }
          ++scan;
        }
        const int64_t span = (record_end < 0) ? (reader_.end() - reader_.pos())
                                              : (record_end - reader_.pos());
        if (static_cast<int64_t>(record_.size()) + span > OB_FILE_SCAN_MAX_RECORD_SIZE) {
          return common::OB_SIZE_OVERFLOW;
        }
        record_.insert(record_.end(), reader_.buf() + reader_.pos(),
                       reader_.buf() + reader_.pos() + span);
        reader_.set_pos(reader_.pos() + span);
        if (record_end < 0) {
          continue;  // 记录尚未结束，继续读下一块
        }
        in_quote_ = false;
        quote_pending_ = false;
        if (!record_.empty() && '\n' == record_.back()) {
          record_.pop_back();
        }
        if (!record_.empty() && '\r' == record_.back()) {
          record_.pop_back();
        }
        if (record_.empty()) {
          record_.clear();
          continue;  // 跳过空行
        }
        if (!bom_checked_) {
          bom_checked_ = true;
          if (record_.size() >= 3 && (uint8_t)record_[0] == 0xEF && (uint8_t)record_[1] == 0xBB
              && (uint8_t)record_[2] == 0xBF) {
            record_.erase(record_.begin(), record_.begin() + 3);
            if (record_.empty()) {
              continue;
            }
          }
        }
        record.swap(record_);
        record_.clear();
        return ret;
      }
      if (reader_.eof()) {
        if (record_.empty()) {
          is_eof = true;
          return ret;
        }
        if (!record_.empty() && '\r' == record_.back()) {
          record_.pop_back();
        }
        record.swap(record_);
        record_.clear();
        is_eof = true;
        return ret;
      }
      if (OB_FAIL(reader_.fill())) {
        ret = common::OB_IO_ERROR;
        return ret;
      }
    }
  }

  void close()
  {
    reader_.close();
    record_.clear();
    in_quote_ = false;
    quote_pending_ = false;
    bom_checked_ = false;
  }

private:
  ObChunkReader reader_;
  std::vector<char> record_;
  bool in_quote_;
  bool quote_pending_;
  bool bom_checked_;
};

// 将一条记录切分为字段；引号字段的 "" 转义就地还原。
int ob_scan_parse_csv_record(char *record, int64_t len, std::vector<ObCsvField> &fields)
{
  int ret = OB_SUCCESS;
  fields.clear();
  int64_t pos = 0;
  while (true) {
    if (pos < len && '"' == record[pos]) {
      const int64_t content_beg = pos + 1;
      int64_t write_pos = content_beg;
      int64_t scan = content_beg;
      bool closed = false;
      while (scan < len) {
        if ('"' == record[scan]) {
          if (scan + 1 < len && '"' == record[scan + 1]) {
            record[write_pos++] = '"';
            scan += 2;
            continue;
          }
          closed = true;
          ++scan;
          break;
        }
        record[write_pos++] = record[scan];
        ++scan;
      }
      if (!closed) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      int64_t tail = scan;
      while (tail < len && (' ' == record[tail] || '\t' == record[tail])) {
        ++tail;
      }
      if (tail < len && ',' != record[tail]) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      ObCsvField field;
      field.offset = content_beg;
      field.length = write_pos - content_beg;
      field.quoted = true;
      fields.push_back(field);
      if (tail >= len) {
        break;
      }
      pos = tail + 1;
    } else {
      const int64_t beg = pos;
      while (pos < len && ',' != record[pos]) {
        ++pos;
      }
      int64_t end = pos;
      if (end > beg && '\r' == record[end - 1]) {
        --end;
      }
      ObCsvField field;
      field.offset = beg;
      field.length = end - beg;
      field.quoted = false;
      fields.push_back(field);
      if (pos >= len) {
        break;
      }
      ++pos;
    }
  }
  return ret;
}

class ObCsvFileScanReader : public ObFileScanReader
{
public:
  ObCsvFileScanReader() : alloc_(NULL), column_count_(0) {}

  int open(common::ObIAllocator &allocator, const common::ObString &path,
           const ObFileScanOptions &options) override
  {
    int ret = OB_SUCCESS;
    alloc_ = &allocator;
    options_ = options;
    if (OB_ISNULL(path.ptr()) || path.length() <= 0) {
      return common::OB_INVALID_ARGUMENT;
    }
    path_.assign(path.ptr(), path.length());
    if (options.sample_size >= 0) {
      return common::OB_NOT_SUPPORTED;  // 仅支持全量推断
    }
    if (OB_FAIL(infer_schema())) {
      return ret;
    }
    if (OB_FAIL(build_columns())) {
      return ret;
    }
    if (OB_FAIL(scanner_.open(path_.c_str()))) {
      return ret;
    }
    if (options_.has_header) {
      std::vector<char> header;
      bool is_eof = false;
      if (OB_FAIL(scanner_.next(header, is_eof))) {
        ret = common::OB_IO_ERROR;
      }
    }
    return ret;
  }

  const common::ObSEArray<ObFileScanColumn, 16> &columns() const override { return columns_; }

  int next_row(common::ObIAllocator &allocator,
               common::ObSEArray<ObFileScanValue, 16> &row) override
  {
    int ret = OB_SUCCESS;
    while (true) {
      bool is_eof = false;
      if (OB_FAIL(scanner_.next(record_, is_eof))) {
        return common::OB_IO_ERROR;
      }
      if (is_eof) {
        return common::OB_ITER_END;
      }
      break;
    }
    if (OB_FAIL(ob_scan_parse_csv_record(record_.data(), static_cast<int64_t>(record_.size()),
                                         fields_))) {
      return common::OB_ERR_UNEXPECTED;
    }
    if (static_cast<int64_t>(fields_.size()) > column_count_) {
      return common::OB_ERR_UNEXPECTED;  // 字段数多于表头
    }
    ob_scan_reset_row(row);
    for (int64_t i = 0; i < column_count_; ++i) {
      ObFileScanValue value;
      if (i < static_cast<int64_t>(fields_.size())) {
        const ObCsvField &field = fields_[i];
        if (!field.quoted && 0 == field.length) {
          value.is_null = true;
        } else {
          if (OB_FAIL(ob_scan_copy_to_allocator(allocator, record_.data() + field.offset,
                                                field.length, value.text))) {
            return ret;
          }
          value.is_null = false;
        }
      } else {
        value.is_null = true;  // 缺失字段视为 NULL
      }
      if (OB_FAIL(row.push_back(value))) {
        return ret;
      }
    }
    return ret;
  }

  void close() override
  {
    scanner_.close();
    columns_.reset();
    column_count_ = 0;
  }

private:
  int infer_schema()
  {
    int ret = OB_SUCCESS;
    ObCsvRecordScanner scanner;
    if (OB_FAIL(scanner.open(path_.c_str()))) {
      ret = common::OB_IO_ERROR;
      return ret;
    }
    std::vector<char> record;
    bool is_eof = false;
    if (OB_FAIL(scanner.next(record, is_eof))) {
      ret = common::OB_IO_ERROR;
      return ret;
    }
    if (is_eof) {
      column_count_ = 0;
      return ret;  // 空文件
    }
    std::vector<ObCsvField> header_fields;
    if (OB_FAIL(ob_scan_parse_csv_record(record.data(), static_cast<int64_t>(record.size()),
                                         header_fields))) {
      ret = common::OB_ERR_UNEXPECTED;
      return ret;
    }
    column_count_ = static_cast<int64_t>(header_fields.size());
    if (column_count_ > OB_FILE_SCAN_MAX_COLUMNS) {
      ret = common::OB_INVALID_ARGUMENT;
      return ret;
    }
    header_names_.clear();
    kinds_.assign(column_count_, SCAN_KIND_UNKNOWN);
    nullables_.assign(column_count_, 0);
    if (options_.has_header) {
      for (int64_t i = 0; i < column_count_; ++i) {
        const ObCsvField &field = header_fields[i];
        header_names_.push_back(std::string(record.data() + field.offset, field.length));
      }
    } else {
      for (int64_t i = 0; i < column_count_; ++i) {
        char name_buf[32];
        snprintf(name_buf, sizeof(name_buf), "col_%lld", static_cast<long long>(i + 1));
        header_names_.push_back(std::string(name_buf));
      }
      // 无表头时首行即数据行
      if (OB_FAIL(merge_data_row(record, header_fields))) {
        return ret;
      }
    }
    // 扫描全部数据行，统计列类型与可空性
    while (true) {
      if (OB_FAIL(scanner.next(record, is_eof))) {
        ret = common::OB_IO_ERROR;
        return ret;
      }
      if (is_eof) {
        break;
      }
      std::vector<ObCsvField> fields;
      if (OB_FAIL(ob_scan_parse_csv_record(record.data(), static_cast<int64_t>(record.size()),
                                           fields))) {
        ret = common::OB_ERR_UNEXPECTED;
        return ret;
      }
      if (OB_FAIL(merge_data_row(record, fields))) {
        return ret;
      }
    }
    return ret;
  }

  // 用一行数据更新各列的类型与可空性
  int merge_data_row(const std::vector<char> &record, const std::vector<ObCsvField> &fields)
  {
    int ret = OB_SUCCESS;
    if (static_cast<int64_t>(fields.size()) > column_count_) {
      return common::OB_ERR_UNEXPECTED;  // 字段数多于列数
    }
    for (int64_t i = 0; i < column_count_; ++i) {
      if (i >= static_cast<int64_t>(fields.size())) {
        nullables_[i] = 1;  // 缺失字段
        continue;
      }
      const ObCsvField &field = fields[i];
      if (!field.quoted && 0 == field.length) {
        nullables_[i] = 1;  // 未加引号的空字段是 NULL
        continue;
      }
      const ObScanKind kind = ob_scan_detect_kind(record.data() + field.offset, field.length);
      kinds_[i] = ob_scan_merge_kind(kinds_[i], (SCAN_KIND_UNKNOWN == kind && field.quoted)
                                                      ? SCAN_KIND_VARCHAR
                                                      : kind);
    }
    return ret;
  }

  int build_columns()
  {
    int ret = OB_SUCCESS;
    columns_.reset();
    std::map<std::string, int64_t> name_counter;
    for (int64_t i = 0; i < column_count_; ++i) {
      const std::string &raw_name = header_names_[i];
      const int64_t seq = ++name_counter[raw_name];
      std::string name = raw_name;
      if (seq > 1) {
        char suffix[32];
        snprintf(suffix, sizeof(suffix), "__%lld", static_cast<long long>(seq));
        name += suffix;
      }
      ObFileScanColumn column;
      if (OB_FAIL(ob_scan_copy_to_allocator(*alloc_, name.c_str(),
                                            static_cast<int64_t>(name.size()), column.name))) {
        return ret;
      }
      column.type = ob_scan_kind_to_type(kinds_[i]);
      column.nullable = (0 != nullables_[i]);
      if (OB_FAIL(columns_.push_back(column))) {
        return ret;
      }
    }
    return ret;
  }

private:
  common::ObIAllocator *alloc_;
  ObFileScanOptions options_;
  std::string path_;
  common::ObSEArray<ObFileScanColumn, 16> columns_;
  ObCsvRecordScanner scanner_;
  std::vector<char> record_;
  std::vector<ObCsvField> fields_;
  std::vector<std::string> header_names_;
  std::vector<ObScanKind> kinds_;
  std::vector<char> nullables_;
  int64_t column_count_;
};

//============================= JSONL =============================

struct ObJsonField {
  std::string key;
  std::string value;
  bool is_null;
  bool is_string;
};

inline void ob_scan_skip_ws(const char *data, int64_t len, int64_t &pos)
{
  while (pos < len && ob_scan_is_space(data[pos])) {
    ++pos;
  }
}

inline void ob_scan_append_utf8(std::string &out, uint32_t code)
{
  if (code < 0x80) {
    out.push_back(static_cast<char>(code));
  } else if (code < 0x800) {
    out.push_back(static_cast<char>(0xC0 | (code >> 6)));
    out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
  } else if (code < 0x10000) {
    out.push_back(static_cast<char>(0xE0 | (code >> 12)));
    out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
  } else {
    out.push_back(static_cast<char>(0xF0 | (code >> 18)));
    out.push_back(static_cast<char>(0x80 | ((code >> 12) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
  }
}

int ob_scan_parse_hex4(const char *data, int64_t len, int64_t &pos, uint32_t &code)
{
  int ret = OB_SUCCESS;
  code = 0;
  if (pos + 4 > len) {
    return common::OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; i < 4; ++i) {
    const char c = data[pos + i];
    uint32_t digit = 0;
    if (c >= '0' && c <= '9') {
      digit = static_cast<uint32_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      digit = static_cast<uint32_t>(c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      digit = static_cast<uint32_t>(c - 'A' + 10);
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      return ret;
    }
    code = (code << 4) | digit;
  }
  pos += 4;
  return ret;
}

int ob_scan_parse_json_string(const char *data, int64_t len, int64_t &pos, std::string &out)
{
  int ret = OB_SUCCESS;
  ++pos;  // 跳过起始引号
  while (pos < len) {
    const char c = data[pos];
    if ('"' == c) {
      ++pos;
      return ret;
    }
    if ('\\' == c) {
      ++pos;
      if (pos >= len) {
        return common::OB_ERR_UNEXPECTED;
      }
      const char escaped = data[pos++];
      switch (escaped) {
        case '"':
          out.push_back('"');
          break;
        case '\\':
          out.push_back('\\');
          break;
        case '/':
          out.push_back('/');
          break;
        case 'b':
          out.push_back('\b');
          break;
        case 'f':
          out.push_back('\f');
          break;
        case 'n':
          out.push_back('\n');
          break;
        case 'r':
          out.push_back('\r');
          break;
        case 't':
          out.push_back('\t');
          break;
        case 'u': {
          uint32_t code = 0;
          if (OB_FAIL(ob_scan_parse_hex4(data, len, pos, code))) {
            return ret;
          }
          if (code >= 0xD800 && code <= 0xDBFF && pos + 1 < len && '\\' == data[pos]
              && 'u' == data[pos + 1]) {
            pos += 2;
            uint32_t low = 0;
            if (OB_FAIL(ob_scan_parse_hex4(data, len, pos, low))) {
              return ret;
            }
            if (low >= 0xDC00 && low <= 0xDFFF) {
              code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
            } else {
              ob_scan_append_utf8(out, code);
              code = low;
            }
          }
          ob_scan_append_utf8(out, code);
          break;
        }
        default:
          return common::OB_ERR_UNEXPECTED;
      }
      continue;
    }
    out.push_back(c);
    ++pos;
  }
  return common::OB_ERR_UNEXPECTED;
}

// 解析一行 JSON 对象；顶层必须是对象，嵌套对象/数组不支持。
int ob_scan_parse_json_object(const char *data, int64_t len, std::vector<ObJsonField> &fields)
{
  int ret = OB_SUCCESS;
  fields.clear();
  int64_t pos = 0;
  ob_scan_skip_ws(data, len, pos);
  if (pos >= len || '{' != data[pos]) {
    return common::OB_NOT_SUPPORTED;
  }
  ++pos;
  ob_scan_skip_ws(data, len, pos);
  if (pos < len && '}' == data[pos]) {
    ++pos;
    ob_scan_skip_ws(data, len, pos);
    return (pos == len) ? OB_SUCCESS : common::OB_ERR_UNEXPECTED;
  }
  while (true) {
    ob_scan_skip_ws(data, len, pos);
    if (pos >= len || '"' != data[pos]) {
      ret = common::OB_ERR_UNEXPECTED;
      break;
    }
    ObJsonField field;
    field.is_null = false;
    field.is_string = false;
    if (OB_FAIL(ob_scan_parse_json_string(data, len, pos, field.key))) {
      ret = common::OB_ERR_UNEXPECTED;
      break;
    }
    ob_scan_skip_ws(data, len, pos);
    if (pos >= len || ':' != data[pos]) {
      ret = common::OB_ERR_UNEXPECTED;
      break;
    }
    ++pos;
    ob_scan_skip_ws(data, len, pos);
    if (pos >= len) {
      ret = common::OB_ERR_UNEXPECTED;
      break;
    }
    const char c = data[pos];
    if ('"' == c) {
      if (OB_FAIL(ob_scan_parse_json_string(data, len, pos, field.value))) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      field.is_string = true;
    } else if ('{' == c || '[' == c) {
      ret = common::OB_NOT_SUPPORTED;  // 不支持嵌套对象/数组
      break;
    } else if ('t' == c) {
      if (pos + 4 > len || 0 != memcmp(data + pos, "true", 4)) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      pos += 4;
      field.value = "true";
    } else if ('f' == c) {
      if (pos + 5 > len || 0 != memcmp(data + pos, "false", 5)) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      pos += 5;
      field.value = "false";
    } else if ('n' == c) {
      if (pos + 4 > len || 0 != memcmp(data + pos, "null", 4)) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      pos += 4;
      field.is_null = true;
    } else {
      const int64_t beg = pos;
      while (pos < len
             && (ob_scan_is_digit(data[pos]) || '-' == data[pos] || '+' == data[pos] || '.'
                 == data[pos] || 'e' == data[pos] || 'E' == data[pos])) {
        ++pos;
      }
      if (beg == pos) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      field.value.assign(data + beg, pos - beg);
      const ObScanKind kind = ob_scan_detect_kind(field.value.c_str(),
                                                  static_cast<int64_t>(field.value.size()));
      if (SCAN_KIND_UNKNOWN == kind || SCAN_KIND_VARCHAR == kind) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
    }
    fields.push_back(field);
    ob_scan_skip_ws(data, len, pos);
    if (pos < len && ',' == data[pos]) {
      ++pos;
      continue;
    }
    if (pos < len && '}' == data[pos]) {
      ++pos;
      break;
    }
    ret = common::OB_ERR_UNEXPECTED;
    break;
  }
  if (OB_SUCC(ret)) {
    ob_scan_skip_ws(data, len, pos);
    if (pos != len) {
      ret = common::OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

// 行扫描器：JSONL 每行一个 JSON 对象，按 '\n' 切分。
class ObLineScanner
{
public:
  ObLineScanner() {}

  int open(const char *path)
  {
    int ret = OB_SUCCESS;
    line_.clear();
    if (OB_FAIL(reader_.open(path))) {
      ret = common::OB_IO_ERROR;
    }
    return ret;
  }

  int next(std::vector<char> &line, bool &is_eof)
  {
    int ret = OB_SUCCESS;
    is_eof = false;
    while (true) {
      if (reader_.pos() < reader_.end()) {
        char *buf = reader_.buf();
        const int64_t remain = reader_.end() - reader_.pos();
        char *newline = static_cast<char *>(memchr(buf + reader_.pos(), '\n', remain));
        if (OB_ISNULL(newline)) {
          line_.insert(line_.end(), buf + reader_.pos(), buf + reader_.end());
          reader_.set_pos(reader_.end());
          continue;
        }
        line_.insert(line_.end(), buf + reader_.pos(), newline);
        reader_.set_pos(newline - buf + 1);
        if (!line_.empty() && '\r' == line_.back()) {
          line_.pop_back();
        }
        if (line_.empty()) {
          line_.clear();
          continue;  // 跳过空行
        }
        line.swap(line_);
        line_.clear();
        return ret;
      }
      if (reader_.eof()) {
        if (line_.empty()) {
          is_eof = true;
          return ret;
        }
        if (!line_.empty() && '\r' == line_.back()) {
          line_.pop_back();
        }
        line.swap(line_);
        line_.clear();
        is_eof = true;
        return ret;
      }
      if (OB_FAIL(reader_.fill())) {
        ret = common::OB_IO_ERROR;
        return ret;
      }
    }
  }

  void close()
  {
    reader_.close();
    line_.clear();
  }

private:
  ObChunkReader reader_;
  std::vector<char> line_;
};

class ObJsonlFileScanReader : public ObFileScanReader
{
public:
  ObJsonlFileScanReader() : alloc_(NULL), column_count_(0) {}

  int open(common::ObIAllocator &allocator, const common::ObString &path,
           const ObFileScanOptions &options) override
  {
    int ret = OB_SUCCESS;
    alloc_ = &allocator;
    options_ = options;
    if (OB_ISNULL(path.ptr()) || path.length() <= 0) {
      return common::OB_INVALID_ARGUMENT;
    }
    path_.assign(path.ptr(), path.length());
    if (options.sample_size >= 0) {
      return common::OB_NOT_SUPPORTED;
    }
    if (OB_FAIL(infer_schema())) {
      return ret;
    }
    if (OB_FAIL(build_columns())) {
      return ret;
    }
    if (OB_FAIL(scanner_.open(path_.c_str()))) {
      return ret;
    }
    return ret;
  }

  const common::ObSEArray<ObFileScanColumn, 16> &columns() const override { return columns_; }

  int next_row(common::ObIAllocator &allocator,
               common::ObSEArray<ObFileScanValue, 16> &row) override
  {
    int ret = OB_SUCCESS;
    while (true) {
      bool is_eof = false;
      if (OB_FAIL(scanner_.next(line_, is_eof))) {
        return common::OB_IO_ERROR;
      }
      if (is_eof) {
        return common::OB_ITER_END;
      }
      if (OB_FAIL(ob_scan_parse_json_object(line_.data(), static_cast<int64_t>(line_.size()),
                                            fields_))) {
        return common::OB_ERR_UNEXPECTED;
      }
      break;
    }
    ob_scan_reset_row(row);
    for (int64_t i = 0; i < column_count_; ++i) {
      ObFileScanValue value;
      const ObJsonField *found = NULL;
      for (int64_t j = 0; j < static_cast<int64_t>(fields_.size()); ++j) {
        if (key_names_[i] == fields_[j].key) {
          found = &fields_[j];
          break;
        }
      }
      if (OB_ISNULL(found) || found->is_null) {
        value.is_null = true;
      } else {
        if (OB_FAIL(ob_scan_copy_to_allocator(allocator, found->value.c_str(),
                                              static_cast<int64_t>(found->value.size()),
                                              value.text))) {
          return ret;
        }
        value.is_null = false;
      }
      if (OB_FAIL(row.push_back(value))) {
        return ret;
      }
    }
    return ret;
  }

  void close() override
  {
    scanner_.close();
    columns_.reset();
    column_count_ = 0;
  }

private:
  int infer_schema()
  {
    int ret = OB_SUCCESS;
    ObLineScanner scanner;
    if (OB_FAIL(scanner.open(path_.c_str()))) {
      ret = common::OB_IO_ERROR;
      return ret;
    }
    key_names_.clear();
    kinds_.clear();
    nullables_.clear();
    key_index_.clear();
    std::vector<char> line;
    std::vector<ObJsonField> fields;
    int64_t line_no = 0;
    while (true) {
      bool is_eof = false;
      if (OB_FAIL(scanner.next(line, is_eof))) {
        ret = common::OB_IO_ERROR;
        return ret;
      }
      if (is_eof) {
        break;
      }
      if (line.empty()) {
        continue;
      }
      if (OB_FAIL(ob_scan_parse_json_object(line.data(), static_cast<int64_t>(line.size()),
                                            fields))) {
        ret = common::OB_ERR_UNEXPECTED;
        return ret;
      }
      std::vector<char> present(kinds_.size(), 0);
      for (int64_t i = 0; i < static_cast<int64_t>(fields.size()); ++i) {
        const ObJsonField &field = fields[i];
        std::map<std::string, int64_t>::iterator iter = key_index_.find(field.key);
        int64_t index = 0;
        if (iter == key_index_.end()) {
          index = static_cast<int64_t>(key_names_.size());
          if (index >= OB_FILE_SCAN_MAX_COLUMNS) {
            ret = common::OB_INVALID_ARGUMENT;
            return ret;
          }
          key_index_[field.key] = index;
          key_names_.push_back(field.key);
          kinds_.push_back(SCAN_KIND_UNKNOWN);
          nullables_.push_back(line_no > 0 ? 1 : 0);  // 之前的行缺少该列
          present.push_back(0);
        } else {
          index = iter->second;
        }
        present[index] = 1;
        if (field.is_null) {
          nullables_[index] = 1;
          continue;
        }
        ObScanKind kind = ob_scan_detect_kind(field.value.c_str(),
                                               static_cast<int64_t>(field.value.size()));
        if (SCAN_KIND_UNKNOWN == kind) {
          kind = SCAN_KIND_VARCHAR;
        }
        kinds_[index] = ob_scan_merge_kind(kinds_[index], kind);
      }
      for (int64_t i = 0; i < static_cast<int64_t>(kinds_.size()); ++i) {
        if (0 == present[i]) {
          nullables_[i] = 1;  // 该行缺少此列
        }
      }
      ++line_no;
    }
    column_count_ = static_cast<int64_t>(key_names_.size());
    return ret;
  }

  int build_columns()
  {
    int ret = OB_SUCCESS;
    columns_.reset();
    for (int64_t i = 0; i < column_count_; ++i) {
      ObFileScanColumn column;
      if (OB_FAIL(ob_scan_copy_to_allocator(*alloc_, key_names_[i].c_str(),
                                            static_cast<int64_t>(key_names_[i].size()),
                                            column.name))) {
        return ret;
      }
      column.type = ob_scan_kind_to_type(kinds_[i]);
      column.nullable = (0 != nullables_[i]);
      if (OB_FAIL(columns_.push_back(column))) {
        return ret;
      }
    }
    return ret;
  }

private:
  common::ObIAllocator *alloc_;
  ObFileScanOptions options_;
  std::string path_;
  common::ObSEArray<ObFileScanColumn, 16> columns_;
  ObLineScanner scanner_;
  std::vector<char> line_;
  std::vector<ObJsonField> fields_;
  std::vector<std::string> key_names_;
  std::vector<ObScanKind> kinds_;
  std::vector<char> nullables_;
  std::map<std::string, int64_t> key_index_;
  int64_t column_count_;
};

//============================= Parquet =============================

enum ObParquetType : int32_t {
  PARQUET_TYPE_BOOLEAN = 0,
  PARQUET_TYPE_INT32 = 1,
  PARQUET_TYPE_INT64 = 2,
  PARQUET_TYPE_INT96 = 3,
  PARQUET_TYPE_FLOAT = 4,
  PARQUET_TYPE_DOUBLE = 5,
  PARQUET_TYPE_BYTE_ARRAY = 6,
  PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY = 7,
};

enum ObParquetEncoding : int32_t {
  PARQUET_ENCODING_PLAIN = 0,
  PARQUET_ENCODING_PLAIN_DICTIONARY = 2,
  PARQUET_ENCODING_RLE = 3,
  PARQUET_ENCODING_BIT_PACKED = 4,
  PARQUET_ENCODING_RLE_DICTIONARY = 8,
};

enum ObParquetCodec : int32_t {
  PARQUET_CODEC_UNCOMPRESSED = 0,
  PARQUET_CODEC_SNAPPY = 1,
};

enum ObParquetPageType : int32_t {
  PARQUET_PAGE_DATA = 0,
  PARQUET_PAGE_INDEX = 1,
  PARQUET_PAGE_DICTIONARY = 2,
  PARQUET_PAGE_DATA_V2 = 3,
};

enum ObParquetRepetition : int32_t {
  PARQUET_REPETITION_REQUIRED = 0,
  PARQUET_REPETITION_OPTIONAL = 1,
  PARQUET_REPETITION_REPEATED = 2,
};

// Parquet 物理类型 -> OB 类型
inline ObObjType ob_parquet_type_to_obj_type(int32_t type)
{
  switch (type) {
    case PARQUET_TYPE_BOOLEAN:
      return common::ObTinyIntType;
    case PARQUET_TYPE_INT32:
    case PARQUET_TYPE_INT64:
      return common::ObIntType;
    case PARQUET_TYPE_INT96:
      return common::ObDateTimeType;
    case PARQUET_TYPE_FLOAT:
    case PARQUET_TYPE_DOUBLE:
      return common::ObDoubleType;
    case PARQUET_TYPE_BYTE_ARRAY:
    case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY:
      return common::ObVarcharType;
    default:
      return common::ObVarcharType;
  }
}

// Thrift compact protocol 类型
enum ObThriftType : int8_t {
  THRIFT_STOP = 0,
  THRIFT_BOOLEAN_TRUE = 1,
  THRIFT_BOOLEAN_FALSE = 2,
  THRIFT_BYTE = 3,
  THRIFT_I16 = 4,
  THRIFT_I32 = 5,
  THRIFT_I64 = 6,
  THRIFT_DOUBLE = 7,
  THRIFT_BINARY = 8,
  THRIFT_LIST = 9,
  THRIFT_SET = 10,
  THRIFT_MAP = 11,
  THRIFT_STRUCT = 12,
};

inline int ob_thrift_read_varint(const char *data, int64_t len, int64_t &pos, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  int shift = 0;
  bool done = false;
  while (pos < len) {
    const uint8_t byte = static_cast<uint8_t>(data[pos++]);
    value |= (static_cast<uint64_t>(byte & 0x7f) << shift);
    if (0 == (byte & 0x80)) {
      done = true;
      break;
    }
    shift += 7;
    if (shift > 63) {
      break;
    }
  }
  if (!done) {
    ret = common::OB_ERR_UNEXPECTED;
  }
  return ret;
}

class ObThriftCompactReader
{
public:
  ObThriftCompactReader(const char *data, int64_t len)
      : data_(data), len_(len), pos_(0), last_field_id_(0)
  {
  }

  int read_struct_begin()
  {
    // thrift compact 协议中每个结构体各自维护字段 id 增量，嵌套时需要保存/恢复父级状态
    field_id_stack_.push_back(last_field_id_);
    last_field_id_ = 0;
    return OB_SUCCESS;
  }

  int read_struct_end()
  {
    if (!field_id_stack_.empty()) {
      last_field_id_ = field_id_stack_.back();
      field_id_stack_.pop_back();
    }
    return OB_SUCCESS;
  }

  int read_field_header(int16_t &field_id, int8_t &type)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      return common::OB_ERR_UNEXPECTED;
    }
    const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
    type = static_cast<int8_t>(byte & 0x0f);
    if (THRIFT_STOP == type) {
      field_id = 0;
      return ret;
    }
    const int8_t delta = static_cast<int8_t>((byte & 0xf0) >> 4);
    if (0 == delta) {
      uint64_t zigzag = 0;
      if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, zigzag))) {
        return ret;
      }
      field_id = static_cast<int16_t>(static_cast<int64_t>(zigzag >> 1) ^ -(int64_t)(zigzag & 1));
    } else {
      field_id = static_cast<int16_t>(last_field_id_ + delta);
    }
    last_field_id_ = field_id;
    return ret;
  }

  int read_i32(int32_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t zigzag = 0;
    if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, zigzag))) {
      return ret;
    }
    value = static_cast<int32_t>(static_cast<int64_t>(zigzag >> 1) ^ -(int64_t)(zigzag & 1));
    return ret;
  }

  int read_i64(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t zigzag = 0;
    if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, zigzag))) {
      return ret;
    }
    value = static_cast<int64_t>(zigzag >> 1) ^ -(int64_t)(zigzag & 1);
    return ret;
  }

  int read_binary(const char *&ptr, int32_t &length)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, size))) {
      return ret;
    }
    if (pos_ + static_cast<int64_t>(size) > len_) {
      return common::OB_ERR_UNEXPECTED;
    }
    ptr = data_ + pos_;
    length = static_cast<int32_t>(size);
    pos_ += static_cast<int64_t>(size);
    return ret;
  }

  int skip_field(int8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case THRIFT_BOOLEAN_TRUE:
      case THRIFT_BOOLEAN_FALSE:
        break;
      case THRIFT_BYTE: {
        if (pos_ >= len_) {
          ret = common::OB_ERR_UNEXPECTED;
        } else {
          ++pos_;
        }
        break;
      }
      case THRIFT_I16:
      case THRIFT_I32:
      case THRIFT_I64: {
        uint64_t value = 0;
        if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, value))) {
          ret = ret;
        }
        break;
      }
      case THRIFT_DOUBLE: {
        if (pos_ + 8 > len_) {
          ret = common::OB_ERR_UNEXPECTED;
        } else {
          pos_ += 8;
        }
        break;
      }
      case THRIFT_BINARY: {
        const char *ptr = NULL;
        int32_t length = 0;
        if (OB_FAIL(read_binary(ptr, length))) {
          ret = ret;
        }
        break;
      }
      case THRIFT_LIST:
      case THRIFT_SET: {
        int8_t elem_type = 0;
        int32_t size = 0;
        if (OB_FAIL(read_list_header(elem_type, size))) {
          ret = ret;
        } else {
          for (int32_t i = 0; i < size && OB_SUCC(ret); ++i) {
            if (OB_FAIL(skip_field(elem_type))) {
              ret = ret;
            }
          }
        }
        break;
      }
      case THRIFT_MAP: {
        int8_t key_type = 0;
        int8_t val_type = 0;
        int32_t size = 0;
        if (OB_FAIL(read_map_header(key_type, val_type, size))) {
          ret = ret;
        } else {
          for (int32_t i = 0; i < size && OB_SUCC(ret); ++i) {
            if (OB_FAIL(skip_field(key_type)) || OB_FAIL(skip_field(val_type))) {
              ret = ret;
            }
          }
        }
        break;
      }
      case THRIFT_STRUCT: {
        read_struct_begin();
        int16_t field_id = 0;
        int8_t field_type = 0;
        while (true) {
          if (OB_FAIL(read_field_header(field_id, field_type))) {
            ret = ret;
            break;
          }
          if (THRIFT_STOP == field_type) {
            break;
          }
          if (OB_FAIL(skip_field(field_type))) {
            ret = ret;
            break;
          }
        }
        read_struct_end();
        break;
      }
      default:
        ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int read_list_header(int8_t &elem_type, int32_t &size)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      return common::OB_ERR_UNEXPECTED;
    }
    const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
    elem_type = static_cast<int8_t>(byte & 0x0f);
    int64_t count = static_cast<int64_t>((byte & 0xf0) >> 4);
    if (15 == count) {
      uint64_t value = 0;
      if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, value))) {
        return ret;
      }
      count = static_cast<int64_t>(value);
    }
    size = static_cast<int32_t>(count);
    return ret;
  }

  int read_map_header(int8_t &key_type, int8_t &val_type, int32_t &size)
  {
    int ret = OB_SUCCESS;
    uint64_t count = 0;
    if (OB_FAIL(ob_thrift_read_varint(data_, len_, pos_, count))) {
      return ret;
    }
    size = static_cast<int32_t>(count);
    key_type = 0;
    val_type = 0;
    if (count > 0) {
      if (pos_ >= len_) {
        return common::OB_ERR_UNEXPECTED;
      }
      const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
      key_type = static_cast<int8_t>((byte & 0xf0) >> 4);
      val_type = static_cast<int8_t>(byte & 0x0f);
    }
    return ret;
  }

  int64_t consumed() const { return pos_; }

private:
  const char *data_;
  int64_t len_;
  int64_t pos_;
  int16_t last_field_id_;
  std::vector<int16_t> field_id_stack_;
};

struct ObParquetSchemaElement {
  int32_t type;
  int32_t type_length;
  int32_t repetition_type;
  int32_t num_children;
  std::string name;
  ObParquetSchemaElement()
      : type(-1), type_length(0), repetition_type(-1), num_children(0)
  {
  }
};

struct ObParquetStatistics {
  int64_t null_count;
  ObParquetStatistics() : null_count(-1) {}
};

struct ObParquetColumnMeta {
  int32_t type;
  int32_t codec;
  int64_t num_values;
  int64_t total_compressed_size;
  int64_t data_page_offset;
  int64_t dictionary_page_offset;
  ObParquetStatistics statistics;
  std::vector<std::string> path_in_schema;
  ObParquetColumnMeta()
      : type(-1),
        codec(PARQUET_CODEC_UNCOMPRESSED),
        num_values(0),
        total_compressed_size(0),
        data_page_offset(0),
        dictionary_page_offset(-1)
  {
  }
};

struct ObParquetColumnChunk {
  ObParquetColumnMeta meta;
};

struct ObParquetRowGroup {
  int64_t num_rows;
  std::vector<ObParquetColumnChunk> columns;
  ObParquetRowGroup() : num_rows(0) {}
};

struct ObParquetFileMeta {
  int32_t version;
  int64_t num_rows;
  std::vector<ObParquetSchemaElement> schema;
  std::vector<ObParquetRowGroup> row_groups;
  ObParquetFileMeta() : version(0), num_rows(0) {}
};

struct ObParquetPageHeader {
  int32_t type;
  int32_t uncompressed_page_size;
  int32_t compressed_page_size;
  int32_t num_values;
  int32_t num_nulls;
  int32_t num_rows;
  int32_t encoding;
  int32_t definition_levels_byte_length;
  int32_t repetition_levels_byte_length;
  int32_t dictionary_num_values;
  bool is_compressed;
  ObParquetPageHeader()
      : type(-1),
        uncompressed_page_size(0),
        compressed_page_size(0),
        num_values(0),
        num_nulls(0),
        num_rows(0),
        encoding(PARQUET_ENCODING_PLAIN),
        definition_levels_byte_length(0),
        repetition_levels_byte_length(0),
        dictionary_num_values(0),
        is_compressed(true)
  {
  }
};

int ob_parquet_parse_statistics(ObThriftCompactReader &reader, ObParquetStatistics &statistics)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    if (3 == field_id) {
      if (OB_FAIL(reader.read_i64(statistics.null_count))) {
        break;
      }
    } else if (OB_FAIL(reader.skip_field(type))) {
      break;
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_schema_element(ObThriftCompactReader &reader, ObParquetSchemaElement &element)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    switch (field_id) {
      case 1: {
        if (OB_FAIL(reader.read_i32(element.type))) {
          break;
        }
        break;
      }
      case 2: {
        if (OB_FAIL(reader.read_i32(element.type_length))) {
          break;
        }
        break;
      }
      case 3: {
        if (OB_FAIL(reader.read_i32(element.repetition_type))) {
          break;
        }
        break;
      }
      case 4: {
        const char *ptr = NULL;
        int32_t length = 0;
        if (OB_FAIL(reader.read_binary(ptr, length))) {
          break;
        }
        element.name.assign(ptr, length);
        break;
      }
      case 5: {
        if (OB_FAIL(reader.read_i32(element.num_children))) {
          break;
        }
        break;
      }
      default: {
        if (OB_FAIL(reader.skip_field(type))) {
          break;
        }
        break;
      }
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_column_meta(ObThriftCompactReader &reader, ObParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    switch (field_id) {
      case 1: {
        if (OB_FAIL(reader.read_i32(meta.type))) {
          break;
        }
        break;
      }
      case 3: {
        int8_t elem_type = 0;
        int32_t size = 0;
        if (OB_FAIL(reader.read_list_header(elem_type, size))) {
          break;
        }
        for (int32_t i = 0; i < size; ++i) {
          const char *ptr = NULL;
          int32_t length = 0;
          if (OB_FAIL(reader.read_binary(ptr, length))) {
            break;
          }
          meta.path_in_schema.push_back(std::string(ptr, length));
        }
        break;
      }
      case 4: {
        if (OB_FAIL(reader.read_i32(meta.codec))) {
          break;
        }
        break;
      }
      case 5: {
        if (OB_FAIL(reader.read_i64(meta.num_values))) {
          break;
        }
        break;
      }
      case 7: {
        if (OB_FAIL(reader.read_i64(meta.total_compressed_size))) {
          break;
        }
        break;
      }
      case 9: {
        if (OB_FAIL(reader.read_i64(meta.data_page_offset))) {
          break;
        }
        break;
      }
      case 11: {
        if (OB_FAIL(reader.read_i64(meta.dictionary_page_offset))) {
          break;
        }
        break;
      }
      case 12: {
        if (OB_FAIL(ob_parquet_parse_statistics(reader, meta.statistics))) {
          break;
        }
        break;
      }
      default: {
        if (OB_FAIL(reader.skip_field(type))) {
          break;
        }
        break;
      }
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_column_chunk(ObThriftCompactReader &reader, ObParquetColumnChunk &chunk)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    if (3 == field_id) {
      if (OB_FAIL(ob_parquet_parse_column_meta(reader, chunk.meta))) {
        break;
      }
    } else if (OB_FAIL(reader.skip_field(type))) {
      break;
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_row_group(ObThriftCompactReader &reader, ObParquetRowGroup &row_group)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    switch (field_id) {
      case 1: {
        int8_t elem_type = 0;
        int32_t size = 0;
        if (OB_FAIL(reader.read_list_header(elem_type, size))) {
          break;
        }
        for (int32_t i = 0; i < size; ++i) {
          ObParquetColumnChunk chunk;
          if (OB_FAIL(ob_parquet_parse_column_chunk(reader, chunk))) {
            break;
          }
          row_group.columns.push_back(chunk);
        }
        break;
      }
      case 3: {
        if (OB_FAIL(reader.read_i64(row_group.num_rows))) {
          break;
        }
        break;
      }
      default: {
        if (OB_FAIL(reader.skip_field(type))) {
          break;
        }
        break;
      }
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_file_meta(ObThriftCompactReader &reader, ObParquetFileMeta &meta)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    switch (field_id) {
      case 1: {
        if (OB_FAIL(reader.read_i32(meta.version))) {
          break;
        }
        break;
      }
      case 2: {
        int8_t elem_type = 0;
        int32_t size = 0;
        if (OB_FAIL(reader.read_list_header(elem_type, size))) {
          break;
        }
        for (int32_t i = 0; i < size; ++i) {
          ObParquetSchemaElement element;
          if (OB_FAIL(ob_parquet_parse_schema_element(reader, element))) {
            break;
          }
          meta.schema.push_back(element);
        }
        break;
      }
      case 3: {
        if (OB_FAIL(reader.read_i64(meta.num_rows))) {
          break;
        }
        break;
      }
      case 4: {
        int8_t elem_type = 0;
        int32_t size = 0;
        if (OB_FAIL(reader.read_list_header(elem_type, size))) {
          break;
        }
        for (int32_t i = 0; i < size; ++i) {
          ObParquetRowGroup row_group;
          if (OB_FAIL(ob_parquet_parse_row_group(reader, row_group))) {
            break;
          }
          meta.row_groups.push_back(row_group);
        }
        break;
      }
      default: {
        if (OB_FAIL(reader.skip_field(type))) {
          break;
        }
        break;
      }
    }
  }
  reader.read_struct_end();
  return ret;
}

int ob_parquet_parse_page_header(ObThriftCompactReader &reader, ObParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  reader.read_struct_begin();
  int16_t field_id = 0;
  int8_t type = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.read_field_header(field_id, type))) {
      break;
    }
    if (THRIFT_STOP == type) {
      break;
    }
    switch (field_id) {
      case 1: {
        if (OB_FAIL(reader.read_i32(header.type))) {
          break;
        }
        break;
      }
      case 2: {
        if (OB_FAIL(reader.read_i32(header.uncompressed_page_size))) {
          break;
        }
        break;
      }
      case 3: {
        if (OB_FAIL(reader.read_i32(header.compressed_page_size))) {
          break;
        }
        break;
      }
      case 5: {  // DataPageHeader
        reader.read_struct_begin();
        int16_t sub_id = 0;
        int8_t sub_type = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.read_field_header(sub_id, sub_type))) {
            break;
          }
          if (THRIFT_STOP == sub_type) {
            break;
          }
          if (1 == sub_id) {
            if (OB_FAIL(reader.read_i32(header.num_values))) {
              break;
            }
          } else if (2 == sub_id) {
            if (OB_FAIL(reader.read_i32(header.encoding))) {
              break;
            }
          } else if (OB_FAIL(reader.skip_field(sub_type))) {
            break;
          }
        }
        reader.read_struct_end();
        break;
      }
      case 7: {  // DictionaryPageHeader
        reader.read_struct_begin();
        int16_t sub_id = 0;
        int8_t sub_type = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.read_field_header(sub_id, sub_type))) {
            break;
          }
          if (THRIFT_STOP == sub_type) {
            break;
          }
          if (1 == sub_id) {
            if (OB_FAIL(reader.read_i32(header.dictionary_num_values))) {
              break;
            }
          } else if (2 == sub_id) {
            if (OB_FAIL(reader.read_i32(header.encoding))) {
              break;
            }
          } else if (OB_FAIL(reader.skip_field(sub_type))) {
            break;
          }
        }
        reader.read_struct_end();
        break;
      }
      case 8: {  // DataPageHeaderV2
        reader.read_struct_begin();
        int16_t sub_id = 0;
        int8_t sub_type = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(reader.read_field_header(sub_id, sub_type))) {
            break;
          }
          if (THRIFT_STOP == sub_type) {
            break;
          }
          switch (sub_id) {
            case 1: {
              if (OB_FAIL(reader.read_i32(header.num_values))) {
                break;
              }
              break;
            }
            case 2: {
              if (OB_FAIL(reader.read_i32(header.num_nulls))) {
                break;
              }
              break;
            }
            case 3: {
              if (OB_FAIL(reader.read_i32(header.num_rows))) {
                break;
              }
              break;
            }
            case 4: {
              if (OB_FAIL(reader.read_i32(header.encoding))) {
                break;
              }
              break;
            }
            case 5: {
              if (OB_FAIL(reader.read_i32(header.definition_levels_byte_length))) {
                break;
              }
              break;
            }
            case 6: {
              if (OB_FAIL(reader.read_i32(header.repetition_levels_byte_length))) {
                break;
              }
              break;
            }
            case 7: {
              if (THRIFT_BOOLEAN_TRUE == sub_type) {
                header.is_compressed = true;
              } else if (THRIFT_BOOLEAN_FALSE == sub_type) {
                header.is_compressed = false;
              } else if (OB_FAIL(reader.skip_field(sub_type))) {
                break;
              }
              break;
            }
            default: {
              if (OB_FAIL(reader.skip_field(sub_type))) {
                break;
              }
              break;
            }
          }
        }
        reader.read_struct_end();
        break;
      }
      default: {
        if (OB_FAIL(reader.skip_field(type))) {
          break;
        }
        break;
      }
    }
  }
  reader.read_struct_end();
  return ret;
}

// snappy raw 解压
int ob_parquet_snappy_uncompress(const char *input, int64_t input_len, std::string &output,
                                  int64_t expected_len)
{
  int ret = OB_SUCCESS;
  output.clear();
  int64_t pos = 0;
  uint64_t uncompressed_len = 0;
  if (OB_FAIL(ob_thrift_read_varint(input, input_len, pos, uncompressed_len))) {
    return ret;
  }
  if (expected_len > 0) {
    output.reserve(static_cast<size_t>(expected_len));
  }
  while (pos < input_len) {
    const uint8_t tag = static_cast<uint8_t>(input[pos++]);
    const uint8_t tag_type = tag & 0x03;
    if (0 == tag_type) {  // literal
      uint64_t literal_len = (tag >> 2) + 1;
      if (literal_len > 60) {
        const int64_t extra = static_cast<int64_t>(literal_len - 60);
        uint64_t value = 0;
        for (int64_t i = 0; i < extra; ++i) {
          if (pos >= input_len) {
            return common::OB_ERR_UNEXPECTED;
          }
          value |= (static_cast<uint64_t>(static_cast<uint8_t>(input[pos++])) << (8 * i));
        }
        literal_len = value + 1;
      }
      if (pos + static_cast<int64_t>(literal_len) > input_len) {
        return common::OB_ERR_UNEXPECTED;
      }
      output.append(input + pos, static_cast<size_t>(literal_len));
      pos += static_cast<int64_t>(literal_len);
    } else {  // copy
      uint64_t copy_len = 0;
      uint64_t offset = 0;
      if (1 == tag_type) {
        if (pos >= input_len) {
          return common::OB_ERR_UNEXPECTED;
        }
        copy_len = 4 + ((tag >> 2) & 0x07);
        offset = (static_cast<uint64_t>(tag >> 5) << 8) | static_cast<uint8_t>(input[pos++]);
      } else if (2 == tag_type) {
        if (pos + 2 > input_len) {
          return common::OB_ERR_UNEXPECTED;
        }
        copy_len = (tag >> 2) + 1;
        offset = static_cast<uint8_t>(input[pos])
                 | (static_cast<uint64_t>(static_cast<uint8_t>(input[pos + 1])) << 8);
        pos += 2;
      } else {
        if (pos + 4 > input_len) {
          return common::OB_ERR_UNEXPECTED;
        }
        copy_len = (tag >> 2) + 1;
        offset = static_cast<uint8_t>(input[pos])
                 | (static_cast<uint64_t>(static_cast<uint8_t>(input[pos + 1])) << 8)
                 | (static_cast<uint64_t>(static_cast<uint8_t>(input[pos + 2])) << 16)
                 | (static_cast<uint64_t>(static_cast<uint8_t>(input[pos + 3])) << 24);
        pos += 4;
      }
      if (0 == offset || offset > output.size()) {
        return common::OB_ERR_UNEXPECTED;
      }
      const size_t start = output.size() - static_cast<size_t>(offset);
      for (uint64_t i = 0; i < copy_len; ++i) {
        output.push_back(output[start + i]);
      }
    }
  }
  return ret;
}

// RLE / bit-packing hybrid 解码
int ob_parquet_rle_decode(const char *data, int64_t len, int64_t &pos, int64_t num_values,
                          int32_t bit_width, std::vector<int64_t> &values)
{
  int ret = OB_SUCCESS;
  if (0 == bit_width) {
    for (int64_t i = 0; i < num_values; ++i) {
      values.push_back(0);
    }
    return ret;
  }
  const int64_t byte_width = (bit_width + 7) / 8;
  while (static_cast<int64_t>(values.size()) < num_values) {
    if (pos >= len) {
      ret = common::OB_ERR_UNEXPECTED;
      break;
    }
    uint64_t header = 0;
    if (OB_FAIL(ob_thrift_read_varint(data, len, pos, header))) {
      break;
    }
    if (0 == (header & 1)) {  // RLE run
      const int64_t run_len = static_cast<int64_t>(header >> 1);
      if (pos + byte_width > len) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      int64_t value = 0;
      for (int64_t i = 0; i < byte_width; ++i) {
        value |= (static_cast<int64_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i));
      }
      pos += byte_width;
      for (int64_t i = 0; i < run_len && static_cast<int64_t>(values.size()) < num_values; ++i) {
        values.push_back(value);
      }
    } else {  // bit-packed run
      const int64_t num_groups = static_cast<int64_t>(header >> 1);
      const int64_t count = num_groups * 8;
      const int64_t bytes = num_groups * bit_width;
      if (pos + bytes > len) {
        ret = common::OB_ERR_UNEXPECTED;
        break;
      }
      int64_t bit_pos = 0;
      for (int64_t i = 0; i < count && static_cast<int64_t>(values.size()) < num_values; ++i) {
        int64_t value = 0;
        for (int64_t b = 0; b < bit_width; ++b) {
          const int64_t abs_bit = bit_pos + b;
          const int64_t byte_idx = pos + abs_bit / 8;
          const int64_t bit_idx = abs_bit % 8;
          if ((static_cast<uint8_t>(data[byte_idx]) >> bit_idx) & 1) {
            value |= (1LL << b);
          }
        }
        values.push_back(value);
        bit_pos += bit_width;
      }
      pos += bytes;
    }
  }
  return ret;
}

inline int32_t ob_parquet_bit_width(int64_t max_value)
{
  int32_t width = 0;
  while (max_value > 0) {
    ++width;
    max_value >>= 1;
  }
  return width;
}

inline void ob_parquet_append_double(std::string &out, double value)
{
  char buf[64];
  int len = 0;
  for (int precision = 15; precision <= 17; ++precision) {
    len = snprintf(buf, sizeof(buf), "%.*g", precision, value);
    if (len > 0 && strtod(buf, NULL) == value) {
      break;
    }
  }
  if (len <= 0) {
    return;
  }
  out.append(buf, len);
}

inline void ob_parquet_append_float(std::string &out, float value)
{
  char buf[64];
  int len = 0;
  for (int precision = 6; precision <= 9; ++precision) {
    len = snprintf(buf, sizeof(buf), "%.*g", precision, static_cast<double>(value));
    if (len > 0 && strtof(buf, NULL) == value) {
      break;
    }
  }
  if (len <= 0) {
    return;
  }
  out.append(buf, len);
}

// 儒略日 + 当日纳秒 -> DATETIME 文本
void ob_parquet_int96_to_text(const char *data, std::string &out)
{
  uint64_t nanos = 0;
  for (int64_t i = 0; i < 8; ++i) {
    nanos |= (static_cast<uint64_t>(static_cast<uint8_t>(data[i])) << (8 * i));
  }
  uint32_t julian_day = 0;
  for (int64_t i = 0; i < 4; ++i) {
    julian_day |= (static_cast<uint32_t>(static_cast<uint8_t>(data[8 + i])) << (8 * i));
  }
  const int64_t days = static_cast<int64_t>(julian_day) - 2440588;
  int64_t z = days + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const uint64_t doe = static_cast<uint64_t>(z - era * 146097);
  const uint64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t y = static_cast<int64_t>(yoe) + era * 400;
  const uint64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const uint64_t mp = (5 * doy + 2) / 153;
  const uint64_t d = doy - (153 * mp + 2) / 5 + 1;
  const uint64_t m = mp < 10 ? mp + 3 : mp - 9;
  const int64_t year = y + (m <= 2 ? 1 : 0);
  const int64_t hour = static_cast<int64_t>(nanos / 3600000000000ULL);
  const int64_t minute = static_cast<int64_t>((nanos / 60000000000ULL) % 60);
  const int64_t second = static_cast<int64_t>((nanos / 1000000000ULL) % 60);
  char buf[64];
  int len = snprintf(buf, sizeof(buf), "%04lld-%02llu-%02llu %02lld:%02lld:%02lld",
                     static_cast<long long>(year), static_cast<unsigned long long>(m),
                     static_cast<unsigned long long>(d), static_cast<long long>(hour),
                     static_cast<long long>(minute), static_cast<long long>(second));
  if (len > 0) {
    out.append(buf, len);
  }
}

// PLAIN 编码解码，结果为文本形式
int ob_parquet_decode_plain(const char *data, int64_t len, int64_t &pos, int64_t num_values,
                            int32_t type, int32_t type_length, std::vector<std::string> &values)
{
  int ret = OB_SUCCESS;
  switch (type) {
    case PARQUET_TYPE_BOOLEAN: {
      const int64_t bytes = (num_values + 7) / 8;
      if (pos + bytes > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        const uint8_t byte = static_cast<uint8_t>(data[pos + i / 8]);
        values.push_back(((byte >> (i % 8)) & 1) ? "1" : "0");
      }
      pos += bytes;
      break;
    }
    case PARQUET_TYPE_INT32: {
      if (pos + num_values * 4 > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        int32_t value = 0;
        for (int64_t b = 0; b < 4; ++b) {
          value |= (static_cast<int32_t>(static_cast<uint8_t>(data[pos + b])) << (8 * b));
        }
        char buf[32];
        const int len2 = snprintf(buf, sizeof(buf), "%d", value);
        values.push_back(std::string(buf, len2 > 0 ? len2 : 0));
        pos += 4;
      }
      break;
    }
    case PARQUET_TYPE_INT64: {
      if (pos + num_values * 8 > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        uint64_t raw = 0;
        for (int64_t b = 0; b < 8; ++b) {
          raw |= (static_cast<uint64_t>(static_cast<uint8_t>(data[pos + b])) << (8 * b));
        }
        char buf[32];
        const int len2 = snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(raw));
        values.push_back(std::string(buf, len2 > 0 ? len2 : 0));
        pos += 8;
      }
      break;
    }
    case PARQUET_TYPE_INT96: {
      if (pos + num_values * 12 > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        std::string text;
        ob_parquet_int96_to_text(data + pos, text);
        values.push_back(text);
        pos += 12;
      }
      break;
    }
    case PARQUET_TYPE_FLOAT: {
      if (pos + num_values * 4 > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        uint32_t raw = 0;
        for (int64_t b = 0; b < 4; ++b) {
          raw |= (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + b])) << (8 * b));
        }
        float value = 0;
        memcpy(&value, &raw, sizeof(value));
        std::string text;
        ob_parquet_append_float(text, value);
        values.push_back(text);
        pos += 4;
      }
      break;
    }
    case PARQUET_TYPE_DOUBLE: {
      if (pos + num_values * 8 > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        uint64_t raw = 0;
        for (int64_t b = 0; b < 8; ++b) {
          raw |= (static_cast<uint64_t>(static_cast<uint8_t>(data[pos + b])) << (8 * b));
        }
        double value = 0;
        memcpy(&value, &raw, sizeof(value));
        std::string text;
        ob_parquet_append_double(text, value);
        values.push_back(text);
        pos += 8;
      }
      break;
    }
    case PARQUET_TYPE_BYTE_ARRAY: {
      for (int64_t i = 0; i < num_values; ++i) {
        if (pos + 4 > len) {
          return common::OB_ERR_UNEXPECTED;
        }
        uint32_t size = 0;
        for (int64_t b = 0; b < 4; ++b) {
          size |= (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + b])) << (8 * b));
        }
        pos += 4;
        if (pos + static_cast<int64_t>(size) > len) {
          return common::OB_ERR_UNEXPECTED;
        }
        values.push_back(std::string(data + pos, size));
        pos += size;
      }
      break;
    }
    case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
      const int64_t width = type_length > 0 ? type_length : 0;
      if (pos + num_values * width > len) {
        return common::OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; i < num_values; ++i) {
        values.push_back(std::string(data + pos, width));
        pos += width;
      }
      break;
    }
    default:
      ret = common::OB_NOT_SUPPORTED;
  }
  return ret;
}

struct ObParquetLeaf {
  std::string name;
  int32_t type;
  int32_t type_length;
  int64_t max_def_level;
  int64_t max_rep_level;
  ObParquetLeaf() : type(-1), type_length(0), max_def_level(0), max_rep_level(0) {}
};

void ob_parquet_collect_leaves(const std::vector<ObParquetSchemaElement> &schema, int64_t &index,
                               int64_t num_children, int64_t max_def_level, int64_t max_rep_level,
                               std::vector<ObParquetLeaf> &leaves)
{
  for (int64_t i = 0; i < num_children && index < static_cast<int64_t>(schema.size()); ++i) {
    const ObParquetSchemaElement &element = schema[index++];
    int64_t def_level = max_def_level;
    int64_t rep_level = max_rep_level;
    if (PARQUET_REPETITION_OPTIONAL == element.repetition_type) {
      def_level += 1;
    } else if (PARQUET_REPETITION_REPEATED == element.repetition_type) {
      def_level += 1;
      rep_level += 1;
    }
    if (element.num_children > 0) {
      ob_parquet_collect_leaves(schema, index, element.num_children, def_level, rep_level, leaves);
    } else {
      ObParquetLeaf leaf;
      leaf.name = element.name;
      leaf.type = element.type;
      leaf.type_length = element.type_length;
      leaf.max_def_level = def_level;
      leaf.max_rep_level = rep_level;
      leaves.push_back(leaf);
    }
  }
}

struct ObParquetCell {
  std::string text;
  bool is_null;
  ObParquetCell() : is_null(false) {}
};

class ObParquetFileScanReader : public ObFileScanReader
{
public:
  ObParquetFileScanReader()
      : alloc_(NULL),
        row_group_index_(0),
        row_index_(0),
        group_row_count_(0),
        group_loaded_(false)
  {
  }

  int open(common::ObIAllocator &allocator, const common::ObString &path,
           const ObFileScanOptions &options) override
  {
    int ret = OB_SUCCESS;
    alloc_ = &allocator;
    options_ = options;
    if (OB_ISNULL(path.ptr()) || path.length() <= 0) {
      return common::OB_INVALID_ARGUMENT;
    }
    path_.assign(path.ptr(), path.length());
    if (options.sample_size >= 0) {
      return common::OB_NOT_SUPPORTED;
    }
    if (OB_FAIL(file_.open(path_.c_str()))) {
      return ret;
    }
    if (OB_FAIL(read_footer())) {
      return ret;
    }
    if (OB_FAIL(parse_footer())) {
      return ret;
    }
    if (OB_FAIL(build_columns())) {
      return ret;
    }
    return ret;
  }

  const common::ObSEArray<ObFileScanColumn, 16> &columns() const override { return columns_; }

  int next_row(common::ObIAllocator &allocator,
               common::ObSEArray<ObFileScanValue, 16> &row) override
  {
    int ret = OB_SUCCESS;
    while (true) {
      if (!group_loaded_) {
        if (row_group_index_ >= static_cast<int64_t>(meta_.row_groups.size())) {
          return common::OB_ITER_END;
        }
        if (OB_FAIL(load_row_group(row_group_index_++))) {
          return ret;
        }
        group_loaded_ = true;
        row_index_ = 0;
      }
      if (row_index_ < group_row_count_) {
        break;
      }
      group_loaded_ = false;
    }
    ob_scan_reset_row(row);
    for (int64_t i = 0; i < static_cast<int64_t>(leaves_.size()); ++i) {
      ObFileScanValue value;
      const ObParquetCell &cell = group_cells_[i][row_index_];
      if (cell.is_null) {
        value.is_null = true;
      } else {
        if (OB_FAIL(ob_scan_copy_to_allocator(allocator, cell.text.c_str(),
                                              static_cast<int64_t>(cell.text.size()), value.text))) {
          return ret;
        }
        value.is_null = false;
      }
      if (OB_FAIL(row.push_back(value))) {
        return ret;
      }
    }
    ++row_index_;
    return ret;
  }

  void close() override
  {
    file_.close();
    columns_.reset();
    group_cells_.clear();
    leaves_.clear();
  }

private:
  int read_footer()
  {
    int ret = OB_SUCCESS;
    const int64_t file_size = file_.size();
    if (file_size < 12) {
      return common::OB_ERR_UNEXPECTED;
    }
    char head[4];
    int64_t read_len = 0;
    if (OB_FAIL(file_.pread(head, 4, 0, read_len)) || 4 != read_len) {
      return common::OB_IO_ERROR;
    }
    if (0 != memcmp(head, "PAR1", 4)) {
      return common::OB_ERR_UNEXPECTED;
    }
    char tail[8];
    if (OB_FAIL(file_.pread(tail, 8, file_size - 8, read_len)) || 8 != read_len) {
      return common::OB_IO_ERROR;
    }
    if (0 != memcmp(tail + 4, "PAR1", 4)) {
      return common::OB_ERR_UNEXPECTED;
    }
    uint32_t footer_len = 0;
    for (int64_t i = 0; i < 4; ++i) {
      footer_len |= (static_cast<uint32_t>(static_cast<uint8_t>(tail[i])) << (8 * i));
    }
    if (0 == footer_len || static_cast<int64_t>(footer_len) > file_size - 8) {
      return common::OB_ERR_UNEXPECTED;
    }
    footer_.resize(footer_len);
    if (OB_FAIL(file_.pread(&footer_[0], footer_len, file_size - 8 - footer_len, read_len))
        || static_cast<uint32_t>(read_len) != footer_len) {
      return common::OB_IO_ERROR;
    }
    return ret;
  }

  int parse_footer()
  {
    int ret = OB_SUCCESS;
    ObThriftCompactReader reader(footer_.c_str(), static_cast<int64_t>(footer_.size()));
    if (OB_FAIL(ob_parquet_parse_file_meta(reader, meta_))) {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int build_columns()
  {
    int ret = OB_SUCCESS;
    leaves_.clear();
    if (meta_.schema.empty()) {
      return common::OB_ERR_UNEXPECTED;
    }
    int64_t index = 1;
    ob_parquet_collect_leaves(meta_.schema, index, meta_.schema[0].num_children, 0, 0, leaves_);
    if (leaves_.empty()) {
      return common::OB_ERR_UNEXPECTED;
    }
    columns_.reset();
    for (int64_t i = 0; i < static_cast<int64_t>(leaves_.size()); ++i) {
      ObFileScanColumn column;
      if (OB_FAIL(ob_scan_copy_to_allocator(*alloc_, leaves_[i].name.c_str(),
                                            static_cast<int64_t>(leaves_[i].name.size()),
                                            column.name))) {
        return ret;
      }
      column.type = ob_parquet_type_to_obj_type(leaves_[i].type);
      column.nullable = true;
      if (!meta_.row_groups.empty() && i < static_cast<int64_t>(meta_.row_groups[0].columns.size())) {
        const ObParquetColumnMeta &meta = meta_.row_groups[0].columns[i].meta;
        if (meta.statistics.null_count >= 0) {
          column.nullable = (meta.statistics.null_count > 0);
        }
      }
      if (OB_FAIL(columns_.push_back(column))) {
        return ret;
      }
    }
    return ret;
  }

  int load_row_group(int64_t group_index)
  {
    int ret = OB_SUCCESS;
    const ObParquetRowGroup &row_group = meta_.row_groups[group_index];
    group_cells_.assign(leaves_.size(), std::vector<ObParquetCell>());
    for (int64_t i = 0; i < static_cast<int64_t>(leaves_.size()); ++i) {
      if (i >= static_cast<int64_t>(row_group.columns.size())) {
        ret = common::OB_ERR_UNEXPECTED;
        return ret;
      }
      if (OB_FAIL(decode_column(row_group.columns[i], leaves_[i], group_cells_[i]))) {
        return ret;
      }
    }
    group_row_count_ = row_group.num_rows;
    return ret;
  }

  int decode_column(const ObParquetColumnChunk &chunk, const ObParquetLeaf &leaf,
                     std::vector<ObParquetCell> &cells)
  {
    int ret = OB_SUCCESS;
    const ObParquetColumnMeta &meta = chunk.meta;
    int64_t offset = (meta.dictionary_page_offset >= 0) ? meta.dictionary_page_offset
                                                        : meta.data_page_offset;
    const int64_t end_offset = meta.data_page_offset + meta.total_compressed_size;
    std::vector<std::string> dictionary;
    bool has_dictionary = false;
    while (static_cast<int64_t>(cells.size()) < meta.num_values) {
      if (offset >= file_.size()) {
        break;
      }
      ObParquetPageHeader header;
      int64_t header_len = 0;
      if (OB_FAIL(read_page_header(offset, header, header_len))) {
        return ret;
      }
      const int64_t page_offset = offset + header_len;
      const int64_t page_size = header.compressed_page_size;
      if (page_size < 0 || page_offset + page_size > file_.size()) {
        ret = common::OB_ERR_UNEXPECTED;
        return ret;
      }
      std::vector<char> page_data(page_size);
      int64_t read_len = 0;
      if (page_size > 0) {
        if (OB_FAIL(file_.pread(&page_data[0], page_size, page_offset, read_len))
            || read_len != page_size) {
          ret = common::OB_IO_ERROR;
          return ret;
        }
      }
      if (PARQUET_PAGE_DICTIONARY == header.type) {
        if (OB_FAIL(decode_dictionary(page_data, header, leaf.type, leaf.type_length, dictionary))) {
          return ret;
        }
        has_dictionary = true;
      } else if (PARQUET_PAGE_DATA == header.type || PARQUET_PAGE_DATA_V2 == header.type) {
        if (OB_FAIL(decode_data_page(page_data, header, leaf, meta.codec, dictionary, has_dictionary,
                                     cells))) {
          return ret;
        }
      }
      offset = page_offset + page_size;
      if (end_offset > 0 && offset >= end_offset
          && static_cast<int64_t>(cells.size()) < meta.num_values) {
        break;  // 该列块的数据页已读完
      }
    }
    return ret;
  }

  int read_page_header(int64_t offset, ObParquetPageHeader &header, int64_t &header_len)
  {
    int ret = OB_SUCCESS;
    int64_t buf_size = 4096;
    std::vector<char> buf;
    while (true) {
      buf.resize(buf_size);
      int64_t read_len = 0;
      if (OB_FAIL(file_.pread(&buf[0], buf_size, offset, read_len)) || read_len <= 0) {
        ret = common::OB_IO_ERROR;
        return ret;
      }
      ObThriftCompactReader reader(&buf[0], read_len);
      const int parse_ret = ob_parquet_parse_page_header(reader, header);
      if (OB_SUCC(parse_ret)) {
        header_len = reader.consumed();
        return ret;
      }
      if (read_len < buf_size || buf_size >= OB_FILE_SCAN_MAX_PAGE_HEADER_SIZE) {
        ret = common::OB_ERR_UNEXPECTED;
        return ret;
      }
      buf_size *= 4;  // 页头可能被截断，扩大缓冲区重试
    }
  }

  int decode_dictionary(const std::vector<char> &page_data, const ObParquetPageHeader &header,
                        int32_t type, int32_t type_length, std::vector<std::string> &dictionary)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    const int64_t len = static_cast<int64_t>(page_data.size());
    if (OB_FAIL(ob_parquet_decode_plain(page_data.data(), len, pos, header.dictionary_num_values,
                                        type, type_length, dictionary))) {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int decode_data_page(const std::vector<char> &page_data, const ObParquetPageHeader &header,
                       const ObParquetLeaf &leaf, int32_t codec,
                       const std::vector<std::string> &dictionary, bool has_dictionary,
                       std::vector<ObParquetCell> &cells)
  {
    int ret = OB_SUCCESS;
    const bool is_v2 = (PARQUET_PAGE_DATA_V2 == header.type);
    const int64_t page_len = static_cast<int64_t>(page_data.size());
    if (leaf.max_rep_level > 0) {
      return common::OB_NOT_SUPPORTED;  // 不支持重复字段
    }
    std::string page_buf;   // V1：解压后的整页（层级 + 取值）
    std::string value_buf;  // V2：解压后的取值区
    const char *levels_data = NULL;
    int64_t levels_len = 0;
    int64_t levels_pos = 0;
    const char *values_data = NULL;
    int64_t values_len = 0;
    int64_t values_pos = 0;
    if (is_v2) {
      // V2：重复层级与定义层级不压缩，位于页数据头部；其后是（可能压缩的）取值区
      const int64_t rep_len = header.repetition_levels_byte_length;
      const int64_t def_len = header.definition_levels_byte_length;
      const int64_t values_offset = rep_len + def_len;
      if (values_offset > page_len) {
        return common::OB_ERR_UNEXPECTED;
      }
      levels_data = page_data.data();
      levels_len = page_len;
      levels_pos = rep_len;
      const int64_t compressed_len = page_len - values_offset;
      const int64_t uncompressed_len = header.uncompressed_page_size - values_offset;
      if (header.is_compressed && PARQUET_CODEC_UNCOMPRESSED != codec) {
        if (OB_FAIL(ob_parquet_snappy_uncompress(page_data.data() + values_offset, compressed_len,
                                                 value_buf, uncompressed_len))) {
          return common::OB_ERR_UNEXPECTED;
        }
      } else {
        value_buf.assign(page_data.data() + values_offset, compressed_len);
      }
      values_data = value_buf.data();
      values_len = static_cast<int64_t>(value_buf.size());
      values_pos = 0;
    } else {
      // V1：整页作为一个整体压缩，层级区与取值区都在解压后的页内
      if (PARQUET_CODEC_UNCOMPRESSED != codec) {
        std::string uncompressed;
        if (OB_FAIL(ob_parquet_snappy_uncompress(page_data.data(), page_len, uncompressed,
                                                 header.uncompressed_page_size))) {
          return common::OB_ERR_UNEXPECTED;
        }
        page_buf = uncompressed;
      } else {
        page_buf.assign(page_data.data(), page_len);
      }
      levels_data = page_buf.data();
      levels_len = static_cast<int64_t>(page_buf.size());
      levels_pos = 0;
      values_data = page_buf.data();
      values_len = static_cast<int64_t>(page_buf.size());
      values_pos = 0;
    }
    // 定义层级
    std::vector<int64_t> def_levels;
    const int32_t def_bit_width = ob_parquet_bit_width(leaf.max_def_level);
    if (leaf.max_def_level > 0) {
      if (OB_FAIL(ob_parquet_rle_decode(levels_data, levels_len, levels_pos, header.num_values,
                                        def_bit_width, def_levels))) {
        return common::OB_ERR_UNEXPECTED;
      }
    }
    if (!is_v2) {
      values_pos = levels_pos;  // V1：取值紧跟在层级区之后
    }
    int64_t null_count = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(def_levels.size()); ++i) {
      if (def_levels[i] < leaf.max_def_level) {
        ++null_count;
      }
    }
    const int64_t value_count = header.num_values - null_count;
    // 取值区
    std::vector<std::string> values;
    if (value_count > 0) {
      if (PARQUET_ENCODING_PLAIN_DICTIONARY == header.encoding
          || PARQUET_ENCODING_RLE_DICTIONARY == header.encoding) {
        if (OB_FAIL(decode_dictionary_indices(values_data, values_len, values_pos, header, leaf,
                                              dictionary, has_dictionary, value_count, values))) {
          return ret;
        }
      } else if (PARQUET_ENCODING_PLAIN == header.encoding) {
        if (OB_FAIL(ob_parquet_decode_plain(values_data, values_len, values_pos, value_count,
                                            leaf.type, leaf.type_length, values))) {
          return common::OB_ERR_UNEXPECTED;
        }
      } else {
        ret = common::OB_NOT_SUPPORTED;
        return ret;
      }
    }
    // 与定义层级合并成单元格
    int64_t value_index = 0;
    for (int64_t i = 0; i < header.num_values; ++i) {
      ObParquetCell cell;
      if (leaf.max_def_level > 0 && i < static_cast<int64_t>(def_levels.size())
          && def_levels[i] < leaf.max_def_level) {
        cell.is_null = true;
      } else {
        if (value_index < static_cast<int64_t>(values.size())) {
          cell.text = values[value_index++];
        }
        cell.is_null = false;
      }
      cells.push_back(cell);
    }
    return ret;
  }

  int decode_dictionary_indices(const char *data, int64_t data_len, int64_t pos,
                                const ObParquetPageHeader &header, const ObParquetLeaf &leaf,
                                const std::vector<std::string> &dictionary, bool has_dictionary,
                                int64_t value_count, std::vector<std::string> &values)
  {
    int ret = OB_SUCCESS;
    if (!has_dictionary || dictionary.empty()) {
      return common::OB_ERR_UNEXPECTED;
    }
    int32_t bit_width = 0;
    if (PARQUET_PAGE_DATA_V2 == header.type) {
      // V2：位宽由字典大小推导，且不存储在页内
      bit_width = ob_parquet_bit_width(static_cast<int64_t>(dictionary.size()));
    } else {
      // V1：位宽是取值区的第一个字节
      if (pos >= data_len) {
        return common::OB_ERR_UNEXPECTED;
      }
      bit_width = static_cast<int8_t>(data[pos++]);
    }
    std::vector<int64_t> indices;
    if (OB_FAIL(ob_parquet_rle_decode(data, data_len, pos, value_count, bit_width, indices))) {
      return common::OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; i < static_cast<int64_t>(indices.size()); ++i) {
      if (indices[i] < 0 || indices[i] >= static_cast<int64_t>(dictionary.size())) {
        return common::OB_ERR_UNEXPECTED;
      }
      values.push_back(dictionary[indices[i]]);
    }
    UNUSED(leaf);
    return ret;
  }

private:
  common::ObIAllocator *alloc_;
  ObFileScanOptions options_;
  std::string path_;
  common::ObSEArray<ObFileScanColumn, 16> columns_;
  ObFileScanFile file_;
  std::string footer_;
  ObParquetFileMeta meta_;
  std::vector<ObParquetLeaf> leaves_;
  std::vector<std::vector<ObParquetCell> > group_cells_;
  int64_t row_group_index_;
  int64_t row_index_;
  int64_t group_row_count_;
  bool group_loaded_;
};

} // namespace

//============================= 工厂 =============================

ObFileScanReader *ObFileScanReaderFactory::create(common::ObIAllocator &allocator,
                                                 ObFileScanFormat format)
{
  ObFileScanReader *reader = NULL;
  void *mem = NULL;
  switch (format) {
    case ObFileScanFormat::CSV:
      mem = allocator.alloc(sizeof(ObCsvFileScanReader));
      break;
    case ObFileScanFormat::JSONL:
      mem = allocator.alloc(sizeof(ObJsonlFileScanReader));
      break;
    case ObFileScanFormat::PARQUET:
      mem = allocator.alloc(sizeof(ObParquetFileScanReader));
      break;
    default:
      break;
  }
  if (OB_ISNULL(mem)) {
    return NULL;
  }
  switch (format) {
    case ObFileScanFormat::CSV:
      reader = new (mem) ObCsvFileScanReader();
      break;
    case ObFileScanFormat::JSONL:
      reader = new (mem) ObJsonlFileScanReader();
      break;
    case ObFileScanFormat::PARQUET:
      reader = new (mem) ObParquetFileScanReader();
      break;
    default:
      break;
  }
  return reader;
}

} // namespace sql
} // namespace oceanbase

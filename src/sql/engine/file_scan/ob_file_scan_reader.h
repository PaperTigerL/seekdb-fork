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

#ifndef OB_FILE_SCAN_READER_H
#define OB_FILE_SCAN_READER_H

#include <stdint.h>
#include "lib/alloc/ob_iallocator.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_obj_type.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{

// 文件格式
enum class ObFileScanFormat : uint8_t { CSV = 0, JSONL = 1, PARQUET = 2 };

// 推断出的列。
// 注意：本代码库 ObObjType 中没有 ObInt64Type / ObBoolType / ObDatetimeType，
// 对应关系为：BIGINT -> ObIntType, BOOLEAN -> ObTinyIntType, DATETIME -> ObDateTimeType。
struct ObFileScanColumn {
  common::ObString name;
  common::ObObjType type;
  bool nullable;
};

// 扫描选项
struct ObFileScanOptions {
  ObFileScanFormat format = ObFileScanFormat::CSV;
  bool has_header = true;          // CSV 首行是否表头，默认 true
  int64_t sample_size = -1;        // 仅支持 -1（全量）；其它值由调用方拒绝
};

// 一行：每个字段的文本形式 + 是否为 NULL
struct ObFileScanValue {
  common::ObString text;   // 文本形式（数字/布尔/日期均按原始文本）
  bool is_null = true;
};

// 流式读取器：打开文件 -> 逐行产出
class ObFileScanReader
{
public:
  virtual ~ObFileScanReader() {}
  // 打开文件并推断 schema（不物化全文件）
  virtual int open(common::ObIAllocator &allocator, const common::ObString &path,
                   const ObFileScanOptions &options) = 0;
  // 推断出的列
  virtual const common::ObSEArray<ObFileScanColumn, 16> &columns() const = 0;
  // 取下一行；无更多行返回 OB_ITER_END
  virtual int next_row(common::ObIAllocator &allocator,
                       common::ObSEArray<ObFileScanValue, 16> &row) = 0;
  virtual void close() {}
};

// 工厂
// 返回的 reader 由 allocator 分配，调用方负责析构并用同一 allocator 释放：
//   reader->close(); reader->~ObFileScanReader(); allocator.free(reader);
class ObFileScanReaderFactory
{
public:
  static ObFileScanReader *create(common::ObIAllocator &allocator,
                                   ObFileScanFormat format);
};

} // namespace sql
} // namespace oceanbase

#endif // OB_FILE_SCAN_READER_H

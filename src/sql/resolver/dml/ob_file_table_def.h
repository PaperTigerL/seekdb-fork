/**
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

#ifndef OB_FILE_TABLE_DEF_H
#define OB_FILE_TABLE_DEF_H

#include "lib/alloc/ob_iallocator.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_obj_type.h"
#include "sql/engine/file_scan/ob_file_scan_reader.h"

namespace oceanbase
{
namespace sql
{

/**
 * 文件表（表函数）的逻辑种类。
 * CSV / JSONL / PARQUET 对应 ObFileScanFormat，GLOB 单独表示目录展开。
 */
enum class ObFileTableFormat : int8_t {
  CSV = 0,
  JSONL = 1,
  PARQUET = 2,
  GLOB = 3,
};

/**
 * 一个文件表列的元数据。解析期由文件内容推断，执行期用于类型转换。
 */
struct ObFileTableColumnDef {
  ObFileTableColumnDef() : type_(ObVarcharType), nullable_(true) {}
  ObFileTableColumnDef(const common::ObString &name, common::ObObjType type, bool nullable)
      : name_(name), type_(type), nullable_(nullable)
  {}
  common::ObString name_;
  common::ObObjType type_;
  bool nullable_;
  TO_STRING_KV(K_(name), K_(type), K_(nullable));
};

/**
 * 文件表定义。解析期由 resolver 构造，随计划序列化给执行期的物理算子。
 */
struct ObFileTableDef {
  ObFileTableDef()
      : format_(ObFileTableFormat::CSV),
        has_header_(true),
        sample_size_(-1),
        row_cnt_(0),
        is_glob_(false)
  {}
  ObFileTableFormat format_;
  common::ObString path_;   // 待扫描的文件路径；GLOB 时为目录/通配模式
  bool has_header_;         // CSV 首行是否表头
  int64_t sample_size_;     // 仅支持 -1（全量扫描）
  int64_t row_cnt_;         // 估计行数，glob 为 0
  bool is_glob_;            // true 表示该表由 glob 展开，列固定为单列 file
  common::ObSEArray<ObFileTableColumnDef, 16, common::ModulePageAllocator, true> columns_;

  // Copy a string into the given allocator, so that the copy does not point
  // into the memory of the source object.
  static int copy_string(const common::ObString &src,
                          common::ObString &dest,
                          common::ObIAllocator &alloc)
  {
    int ret = OB_SUCCESS;
    if (src.empty()) {
      dest.reset();
    } else {
      char *buf = static_cast<char *>(alloc.alloc(src.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(buf, src.ptr(), src.length());
        dest.assign(buf, src.length());
      }
    }
    return ret;
  }

  int deep_copy(const ObFileTableDef &other, common::ObIAllocator *allocator)
  {
    int ret = OB_SUCCESS;
    format_ = other.format_;
    has_header_ = other.has_header_;
    sample_size_ = other.sample_size_;
    row_cnt_ = other.row_cnt_;
    is_glob_ = other.is_glob_;
    columns_.reset();
    if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(copy_string(other.path_, path_, *allocator))) {
    } else if (other.columns_.count() > 0 && OB_FAIL(columns_.reserve(other.columns_.count()))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.columns_.count(); ++i) {
        ObFileTableColumnDef col;
        if (OB_FAIL(copy_string(other.columns_.at(i).name_, col.name_, *allocator))) {
        } else {
          col.type_ = other.columns_.at(i).type_;
          col.nullable_ = other.columns_.at(i).nullable_;
          if (OB_FAIL(columns_.push_back(col))) {
          }
        }
      }
    }
    return ret;
  }
  TO_STRING_KV("format", static_cast<int32_t>(format_),
               K_(path),
               K_(has_header),
               K_(sample_size),
               K_(row_cnt),
               K_(is_glob),
               K_(columns));
};

} // namespace sql
} // namespace oceanbase

#endif // OB_FILE_TABLE_DEF_H

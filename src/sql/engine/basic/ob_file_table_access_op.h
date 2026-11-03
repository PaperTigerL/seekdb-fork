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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_TABLE_ACCESS_OP_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_TABLE_ACCESS_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/dml/ob_file_table_def.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{
class ObPhyOpSeriCtx;

/**
 * 文件表扫描算子的物理规格。
 *
 * 这里保存的是执行期真正需要的信息：文件路径、格式、CSV 表头开关、glob 标记，
 * 以及每个输出列对应的运行时表达式（列类型由表达式自身的 datum_meta_ 决定，
 * 与 ObFileTableDef::columns_[i].type_ 一致）。
 */
class ObFileTableAccessSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileTableAccessSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      column_exprs_(alloc),
      format_(static_cast<int32_t>(ObFileTableFormat::CSV)),
      has_header_(true),
      sample_size_(-1),
      row_cnt_(0),
      is_glob_(false),
      path_() {}
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(format), K_(has_header), K_(is_glob), K_(path));
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
  int32_t format_;        // ObFileTableFormat
  bool has_header_;       // CSV 首行是否表头
  int64_t sample_size_;   // 仅支持 -1
  int64_t row_cnt_;       // 估计行数
  bool is_glob_;          // true 表示 glob 展开，输出单列 file
  common::ObString path_; // 待扫描文件路径；glob 时为目录/通配模式
private:
  DISALLOW_COPY_AND_ASSIGN(ObFileTableAccessSpec);
};

/**
 * 文件表扫描物理算子。
 *
 * CSV / JSONL / Parquet 走 ObFileScanReaderFactory，逐行流式读取，不物化整个文件；
 * GLOB 展开目录下的文件列表，每行输出一个完整路径。
 */
class ObFileTableAccessOp : public ObOperator
{
public:
  ObFileTableAccessOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override { ObOperator::destroy(); }
private:
  int open_data_source();
  int close_data_source();
  int expand_glob_files();
  int calc_next_row();
  int calc_glob_row();
  int set_datum_from_text(ObExpr *dst_expr,
                          const common::ObString &text,
                          common::ObObjType type);
  int set_null_datum(ObExpr *dst_expr);
private:
  ObFileScanReader *reader_;
  common::ObArenaAllocator schema_alloc_;  // reader 与 schema 文本的生命周期
  common::ObArenaAllocator row_alloc_;     // 每行文本的生命周期，逐行回收
  common::ObSEArray<ObFileScanValue, 16> row_;
  common::ObSEArray<common::ObString, 16> glob_files_;
  int64_t glob_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObFileTableAccessOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif

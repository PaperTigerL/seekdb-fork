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

#ifndef _OB_LOG_FILE_TABLE_ACCESS_H
#define _OB_LOG_FILE_TABLE_ACCESS_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
/**
 * 逻辑算子：文件表扫描。
 *
 * 对应 resolver 产出的 TableItem::FILE_TABLE（read_csv / read_json / read_parquet / glob）。
 * table_def_ 指向 resolver 产出的 ObFileTableDef，随 stmt 生命周期存在（与
 * ObLogValuesTableAccess::table_def_ 的做法一致），本算子不持有其所有权。
 * column_exprs_ 为该文件表的所有输出列（ObColumnRefRawExpr），列序与
 * ObFileTableDef::columns_ 一致。
 */
class ObLogFileTableAccess : public ObLogicalOperator
{
public:
  ObLogFileTableAccess(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        column_exprs_(),
        table_def_(NULL),
        table_name_(),
        table_id_(common::OB_INVALID_ID) {}
  virtual ~ObLogFileTableAccess() {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  virtual int compute_equal_set() override;
  virtual int compute_table_set() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;

  int allocate_dummy_output();

  const common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() const { return column_exprs_; }
  common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() { return column_exprs_; }
  inline const ObFileTableDef *get_file_table_def() const { return table_def_; }
  inline void set_file_table_def(ObFileTableDef *table_def) { table_def_ = table_def; }
  inline common::ObString &get_table_name() { return table_name_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
private:
  common::ObSEArray<ObColumnRefRawExpr*, 16, common::ModulePageAllocator, true> column_exprs_;
  ObFileTableDef *table_def_;
  common::ObString table_name_;
  uint64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFileTableAccess);
};
}
}
#endif

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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_AI_COLUMN_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_AI_COLUMN_H_

#include "lib/container/ob_array.h"
#include "lib/ob_define.h"
#include "observer/virtual_table/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

/**
 * The oceanbase.__all_ai_column virtual table.
 *
 * It exposes the metadata of every AI column of the tenant.  The metadata of an
 * AI column is persisted in the comment of the AI column, see
 * sql/resolver/ddl/ob_ai_column_resolver.h, so the virtual table does not need
 * any extra inner table to store the metadata.
 */
class ObAllVirtualAiColumn : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualAiColumn();
  virtual ~ObAllVirtualAiColumn();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  enum DESC_COLUMN {
    TABLE_ID = common::OB_APP_MIN_COLUMN_ID,
    COLUMN_ID,
    COLUMN_NAME,
    MAX_RETRIES,
    NORMALIZED_EXPR,
    STATE_COLUMN_ID,
    REQUEST_COLUMN_ID,
    RESULT_SPEC_COLUMN_ID,
    AI_MODEL_NAME,
    AI_FUNC_TYPE,
    SYNC_MODE,
    VECTOR_DIM,
    RESULT_TYPE,
  };

  int collect_table_ids();
  int next_ai_column(const share::schema::ObTableSchema *&table_schema,
                     const share::schema::ObColumnSchemaV2 *&column);
  int fill_row_cells(const share::schema::ObTableSchema &table_schema,
                     const share::schema::ObColumnSchemaV2 &column);

private:
  bool inited_;
  int64_t table_idx_;
  int64_t column_idx_;
  common::ObArray<uint64_t> table_ids_;
  const share::schema::ObTableSchema *cur_table_schema_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualAiColumn);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_AI_COLUMN_H_

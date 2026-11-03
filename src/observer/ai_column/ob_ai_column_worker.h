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

#ifndef OCEANBASE_OBSERVER_AI_COLUMN_OB_AI_COLUMN_WORKER_H_
#define OCEANBASE_OBSERVER_AI_COLUMN_OB_AI_COLUMN_WORKER_H_

#include "common/mysqlclient/ob_mysql_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/task/ob_timer.h"
#include "lib/task/ob_timer_service.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace observer
{

/* one task of oceanbase.__all_ai_column_task */
struct ObAiColumnTaskInfo
{
  ObAiColumnTaskInfo()
    : table_id_(0), column_id_(0), state_(0), retry_count_(0),
      request_id_(), rowkey_predicate_(), request_payload_()
  {}
  uint64_t table_id_;
  int64_t column_id_;
  int64_t state_;
  int64_t retry_count_;
  /* the id of the group of the tasks, all the AI columns of one row share it */
  common::ObString request_id_;
  /* the text of the predicate which locates the row, eg. "`id` = 2" */
  common::ObString rowkey_predicate_;
  /* the text of the request which was sent to the AI model, it is stored so
   * that a retry sends exactly the same request */
  common::ObString request_payload_;
};

/**
 * The background worker of the AI columns.
 *
 * The worker polls the tasks of oceanbase.__all_ai_column_task, sends the
 * requests of the AI models, writes the results back into the tables which own
 * the AI columns and removes the tasks.
 *
 * The tasks of one request id belong to the AI columns of one row, they are
 * published together so that the results of the AI columns of one row are
 * either all visible or all invisible.
 *
 * The worker is the only writer of the results, so it can protect the results
 * against the late answers of the requests which were sent for an older version
 * of the row (the stale guard).
 */
class ObAiColumnWorker : public common::ObTimerTask
{
public:
  ObAiColumnWorker();
  virtual ~ObAiColumnWorker();
  int init(common::ObMySQLProxy *sql_proxy);
  int start();
  void stop();
  void destroy();
  virtual void runTimerTask() override;

  /* the period of the polling of the tasks */
  static const int64_t WORK_PERIOD_US = 100 * 1000;
  /* the maximum number of the tasks which are processed by one round */
  static const int64_t MAX_TASK_NUM = 32;
  /* the delay of a retry, it gives the observer the time to see the retrying
   * state of the task before the request is sent again */
  static const int64_t RETRY_DELAY_US = 1000 * 1000;

private:
  int do_work();
  int poll_tasks(common::ObIAllocator &allocator,
                common::ObSEArray<ObAiColumnTaskInfo, 32> &tasks);
  int process_task_group(common::ObArenaAllocator &allocator,
                         common::ObSEArray<ObAiColumnTaskInfo, 32> &tasks,
                         const int64_t begin,
                         const int64_t end);

private:
  common::ObTimer timer_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_AI_COLUMN_OB_AI_COLUMN_WORKER_H_

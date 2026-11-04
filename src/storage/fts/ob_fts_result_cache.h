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

#ifndef OCEANBASE_STORAGE_FTS_OB_FTS_RESULT_CACHE_H_
#define OCEANBASE_STORAGE_FTS_OB_FTS_RESULT_CACHE_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace oceanbase
{
namespace storage
{

// Monotonic counter bumped whenever FTS index content may have changed.
// Cached MATCH results are only reused while the generation is unchanged.
class ObFtsContentVersion
{
public:
  static ObFtsContentVersion &instance()
  {
    static ObFtsContentVersion version;
    return version;
  }

  uint64_t get() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return version_;
  }

  void bump()
  {
    std::lock_guard<std::mutex> guard(lock_);
    ++version_;
  }

private:
  mutable std::mutex lock_;
  uint64_t version_ = 1;
};

struct ObFtsCachedRow
{
  bool is_null_ = false;
  bool is_int_ = false;
  int64_t int_value_ = 0;
  double relevance_ = 0.0;
  std::string str_;
};

struct ObFtsCachedResult
{
  std::vector<ObFtsCachedRow> rows_;
  double max_score_ = 0.0;
  int64_t expire_ts_ = 0;
};

// Cross-statement cache of the doc-id stream produced by a full-text scan.
// The key contains the inv-index tablet, the content generation, the scan
// mode and the query tokens, so entries are never shared between different
// queries or index contents.
class ObFtsResultCache
{
public:
  static constexpr int64_t MAX_ENTRIES = 64;
  static constexpr int64_t MAX_TOTAL_ROWS = 262144;
  static constexpr int64_t TTL_MS = 10 * 60 * 1000;

  static ObFtsResultCache &instance()
  {
    static ObFtsResultCache cache;
    return cache;
  }

  bool get(const std::string &key, std::shared_ptr<const ObFtsCachedResult> &result)
  {
    bool hit = false;
    const int64_t now = now_ms();
    std::lock_guard<std::mutex> guard(lock_);
    std::unordered_map<std::string, std::shared_ptr<const ObFtsCachedResult>>::iterator iter =
        map_.find(key);
    if (iter != map_.end()) {
      if (iter->second->expire_ts_ > now) {
        result = iter->second;
        hit = true;
      } else {
        total_rows_ -= static_cast<int64_t>(iter->second->rows_.size());
        map_.erase(iter);
      }
    }
    return hit;
  }

  void put(const std::string &key, std::shared_ptr<ObFtsCachedResult> result)
  {
    if (nullptr == result) {
      return;
    }
    const int64_t rows = static_cast<int64_t>(result->rows_.size());
    if (rows > MAX_TOTAL_ROWS) {
      return;
    }
    result->expire_ts_ = now_ms() + TTL_MS;
    std::lock_guard<std::mutex> guard(lock_);
    if (map_.size() >= MAX_ENTRIES || total_rows_ + rows > MAX_TOTAL_ROWS) {
      map_.clear();
      total_rows_ = 0;
    }
    std::unordered_map<std::string, std::shared_ptr<const ObFtsCachedResult>>::iterator iter =
        map_.find(key);
    if (iter != map_.end()) {
      total_rows_ -= static_cast<int64_t>(iter->second->rows_.size());
      map_.erase(iter);
    }
    map_[key] = std::move(result);
    total_rows_ += rows;
  }

private:
  static int64_t now_ms()
  {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<const ObFtsCachedResult>> map_;
  int64_t total_rows_ = 0;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_OB_FTS_RESULT_CACHE_H_

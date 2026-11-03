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

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/ob_ik_ft_parser.h"

#include "lib/charset/ob_charset.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "data_plane/fts/ob_fts_struct.h"
#include "data_plane/fts/ob_fts_parser_helper.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_cjk_processor.h"
#include "storage/fts/ik/ob_ik_letter_processor.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_quantifier_processor.h"
#include "storage/fts/ik/ob_ik_surrogate_processor.h"

namespace oceanbase
{
namespace storage
{
int ObIKFTParser::init(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    coll_type_ = ObCollationType::CS_TYPE_INVALID;
    if (OB_ISNULL(param.cs_) || OB_ISNULL(param.cs_->name)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (CS_TYPE_INVALID == (coll_type_ = ObCharset::collation_type(param.cs_->name))) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(init_dict(param))) {
    } else if (OB_FAIL(init_ctx(param))) {
    } else if (OB_FAIL(init_segmenter(param))) {
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObIKFTParser::reuse(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    /* the dictionaries and their caches are kept, the context and the
     * processors of the previous text are dropped */
    segmenters_.clear();
    ctx_ = nullptr;
    call_alloc_.reset();
    if (OB_FAIL(init_ctx(param))) {
    } else if (OB_FAIL(init_segmenter(param))) {
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObIKFTParser::get_next_token(const char *&word,
                                 int64_t &word_len,
                                 int64_t &char_cnt,
                                 int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  const char *output_word;
  int64_t len;
  int64_t offset;
  int64_t cnt;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool accept_token = false;
    while (OB_SUCC(ret) && !accept_token) {
      if (OB_FAIL(produce())) {
      } else if (OB_FAIL(ctx_->get_next_token(output_word, len, offset, cnt))) {
        if (OB_ITER_END == ret) {
          // ok, end this iter
        } else {
        }
      } else {
        bool is_stop = false;
        // if (!OB_ISNULL(dict_stop_)
        //     && OB_FAIL(dict_stop_->match(ObString(len, output_word + offset), is_stop))) {
        //   LOG_WARN("Failed to match stopwords", K(ret));
        // } else
        if (!is_stop) {
          word = output_word + offset;
          word_len = len;
          char_cnt = cnt;
          word_freq = 1;
          accept_token = true;
        } else {
        }
      }
    }
  }

  return ret;
}

int ObIKFTParser::produce()
{
  int ret = OB_SUCCESS;
  // Loop until end or has data to output
  while (OB_SUCC(ret) && ctx_->result_list().empty() && !ctx_->iter_end()) {
    if (OB_FAIL(process_next_batch())) {
      if (OB_ITER_END == ret) {
        // ok
      } else {
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIKFTParser::process_one_char(TokenizeContext &ctx,
                                   const char *ch,
                                   const uint8_t char_len,
                                   const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  // The caller has already fetched the current char and its type, pass them
  // straight to each processor instead of re-reading the context per processor.
  for (ObList<ObIIKProcessor *, ObIAllocator>::iterator iter = segmenters_.begin();
       OB_SUCC(ret) && iter != segmenters_.end();
       iter++) {
    if (OB_FAIL((*iter)->do_process(ctx, ch, char_len, type))) {
    }
  }
  return ret;
}

int ObIKFTParser::process_next_batch()
{
  int ret = OB_SUCCESS;
  ctx_->reset_resource();

  // handle next segmenter
  bool do_seg = false;

  if (ctx_->iter_end()) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !do_seg && !ctx_->iter_end()) {
      const char *ch;
      uint8_t char_len = 0;
      ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
      if (OB_FAIL(ctx_->current_char(ch, char_len))) {
      } else if (OB_FAIL(ctx_->current_char_type(type))) {
      } else if (OB_FAIL(process_one_char(*ctx_, ch, char_len, type))) {
      } else {
        // 1. check segmention
        if (ctx_->handle_size() > SEGMENT_LIMIT && type == ObFTCharUtil::CharType::USELESS) {
          do_seg = true;
        }

        // 2. move to next;
        if (OB_FAIL(ctx_->step_next())) {
          if (OB_ITER_END == ret) {
          } else {
          }
        }
      } // end of one batch
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      ObIKArbitrator arb;
      if (OB_FAIL(arb.process(*ctx_))) {
      } else if (OB_FAIL(arb.output_result(*ctx_))) {
      }
    } else {
      // Already logged.
    }
  }

  return ret;
}

namespace {

/* The IK dictionaries are loaded once per parser and the same parser can be
 * reused for the next text. The parsers are pooled at process level because
 * the statements of one session may be executed by different worker threads. */
struct ObIKParserPoolEntry
{
  ObArenaAllocator alloc_;
  ObIKFTParser *parser_;
  bool in_use_;
  ObString coll_name_;
  int64_t parser_version_;
  int64_t mode_;
  ObString main_dict_;
  ObString quan_dict_;
  ObString stopword_dict_;

  ObIKParserPoolEntry()
    : alloc_(ObMemAttr("IKParserCache")),
      parser_(nullptr),
      in_use_(false),
      coll_name_(),
      parser_version_(-1),
      mode_(-1),
      main_dict_(),
      quan_dict_(),
      stopword_dict_()
  {
  }
};

class ObIKParserPool
{
public:
  static const int64_t MAX_ENTRIES = 16;
  static ObIKParserPool &instance()
  {
    static ObIKParserPool pool;
    return pool;
  }

  int acquire(ObFTParserParam &param, ObFTDictHub *hub, ObITokenIterator *&iter)
  {
    int ret = OB_SUCCESS;
    const ObString coll_name(static_cast<int32_t>(strlen(param.cs_->name)), param.cs_->name);
    const int64_t mode = static_cast<int64_t>(param.ik_param_.mode_);
    ObIKParserPoolEntry *entry = nullptr;
    lock_.lock();
    /* look for an idle parser which was built for the same configuration */
    for (int64_t i = 0; OB_ISNULL(entry) && i < count_; ++i) {
      ObIKParserPoolEntry &candidate = entries_[i];
      if (!candidate.in_use_ && OB_NOT_NULL(candidate.parser_)
          && candidate.coll_name_ == coll_name
          && candidate.parser_version_ == param.parser_version_
          && candidate.mode_ == mode
          && candidate.main_dict_ == param.ik_param_.main_dict_
          && candidate.quan_dict_ == param.ik_param_.quan_dict_
          && candidate.stopword_dict_ == param.ik_param_.stopword_dict_) {
        entry = &candidate;
        entry->in_use_ = true;
      }
    }
    lock_.unlock();
    if (OB_NOT_NULL(entry)) {
      if (OB_FAIL(entry->parser_->reuse(param))) {
        entry->parser_->~ObIKFTParser();
        entry->parser_ = nullptr;
        entry->alloc_.reset();
        lock_.lock();
        entry->in_use_ = false;
        lock_.unlock();
      } else {
        iter = entry->parser_;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(iter)) {
      /* build a parser for this configuration, it joins the pool when there is
       * a free slot so the next text can reuse it */
      ObIKParserPoolEntry *slot = nullptr;
      lock_.lock();
      if (count_ < MAX_ENTRIES) {
        slot = &entries_[count_++];
        slot->in_use_ = true;
      }
      lock_.unlock();
      ObIAllocator *alloc = OB_NOT_NULL(slot) ? &slot->alloc_ : param.allocator_;
      ObIKFTParser *parser = nullptr;
      if (OB_ISNULL(parser = OB_NEWx(ObIKFTParser, alloc, *alloc, hub))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(parser->init(param))) {
        OB_DELETEx(ObIKFTParser, alloc, parser);
      } else if (OB_ISNULL(slot)) {
        iter = parser;
      } else {
        slot->parser_ = parser;
        slot->coll_name_.reset();
        slot->main_dict_.reset();
        slot->quan_dict_.reset();
        slot->stopword_dict_.reset();
        const bool copied =
            OB_SUCC(common::ob_write_string(slot->alloc_, coll_name, slot->coll_name_))
            && OB_SUCC(common::ob_write_string(slot->alloc_, param.ik_param_.main_dict_,
                                               slot->main_dict_))
            && OB_SUCC(common::ob_write_string(slot->alloc_, param.ik_param_.quan_dict_,
                                               slot->quan_dict_))
            && OB_SUCC(common::ob_write_string(slot->alloc_, param.ik_param_.stopword_dict_,
                                               slot->stopword_dict_));
        slot->parser_version_ = param.parser_version_;
        slot->mode_ = mode;
        if (!copied) {
          slot->parser_->~ObIKFTParser();
          slot->parser_ = nullptr;
          slot->alloc_.reset();
        } else {
          iter = parser;
        }
      }
      if (OB_FAIL(ret) || (OB_NOT_NULL(slot) && OB_ISNULL(slot->parser_))) {
        if (OB_NOT_NULL(slot)) {
          slot->alloc_.reset();
          lock_.lock();
          slot->in_use_ = false;
          count_--;
          lock_.unlock();
        }
      }
    }
    return ret;
  }

  bool release(ObITokenIterator *iter)
  {
    bool released = false;
    lock_.lock();
    for (int64_t i = 0; !released && i < count_; ++i) {
      if (entries_[i].parser_ == iter) {
        entries_[i].in_use_ = false;
        released = true;
      }
    }
    lock_.unlock();
    return released;
  }

private:
  ObIKParserPool() : lock_(), count_(0) {}
  ObSpinLock lock_;
  ObIKParserPoolEntry entries_[MAX_ENTRIES];
  int64_t count_;
};

} // namespace

int ObIKFTParserDesc::segment(ObFTParserParam *param, ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObFTDictHub *hub = nullptr;
  ObIKParserPool &pool = ObIKParserPool::instance();
  if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObFTParseData::instance().get_dict_hub(hub))) {
  } else if (OB_ISNULL(param->cs_) || OB_ISNULL(param->cs_->name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(pool.acquire(*param, hub, iter))) {
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    /* the parser of the pool stays alive, a parser built outside the pool is
     * owned by this text and released in free_token_iter() */
  }
  return ret;
}

void ObIKFTParserDesc::free_token_iter(ObFTParserParam *param,
                                       ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    if (ObIKParserPool::instance().release(iter)) {
      iter = nullptr;
    } else {
      iter->~ObITokenIterator();
      param->allocator_->free(iter);
    }
  }
}


int ObIKFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

int ObIKFTParser::init_dict(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  ObIFTDict *tmp_dict = nullptr;

  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
  }

  ObFTRangeDict *dict = nullptr;
  ObFTDictDesc main_dict_desc("main_dict",
                              ObFTDictType::DICT_IK_MAIN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc quan_dict_desc("quan_dict",
                              ObFTDictType::DICT_IK_QUAN,
                              ObCharsetType::CHARSET_UTF8MB4,
                              ObCollationType::CS_TYPE_UTF8MB4_BIN);

  ObFTDictDesc stopword_dict_desc("stopword",
                                  ObFTDictType::DICT_IK_STOP,
                                  ObCharsetType::CHARSET_UTF8MB4,
                                  ObCollationType::CS_TYPE_UTF8MB4_BIN);

  if (should_read_newest_table()) {
    // clear dict cache, always false now
  } else {
    if (OB_FAIL(init_single_dict(main_dict_desc, cache_main_))) {
    } else if (OB_FAIL(init_single_dict(quan_dict_desc, cache_quan_))) {
    } else if (OB_FAIL(init_single_dict(stopword_dict_desc, cache_stop_))) {
    }
  }

  if (OB_FAIL(ret)) {
    // already logged.
  } else if (OB_FAIL(build_dict_from_cache(main_dict_desc, cache_main_, dict_main_))) {
  } else if (OB_FAIL(build_dict_from_cache(quan_dict_desc, cache_quan_, dict_quan_))) {
  } else if (OB_FAIL(build_dict_from_cache(stopword_dict_desc, cache_stop_, dict_stop_))) {
  }

  return ret;
}

int ObIKFTParser::init_single_dict(ObFTDictDesc desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hub_->load_cache(desc, container))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(hub_->build_cache(desc, container))) {
      }
    } else {
    }
  }
  return ret;
}

int ObIKFTParser::init_ctx(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;

  if (coll_type_ == common::CS_TYPE_INVALID) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_ = OB_NEWx(TokenizeContext,
                                      &call_alloc_,
                                      coll_type_,
                                      call_alloc_,
                                      param.fulltext_,
                                      param.ft_length_,
                                      param.ik_param_.mode_ == ObFTIKParam::Mode::SMART))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ctx_->init())) {
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(TokenizeContext, &call_alloc_, ctx_);
  }
  return ret;
}

int ObIKFTParser::init_segmenter(const ObFTParserParam &param)
{
  int ret = OB_SUCCESS;
  // do have an order
  ObIKLetterProcessor *letter_seg = nullptr;
  ObIKQuantifierProcessor *cnqsg = nullptr;
  ObIKCJKProcessor *cjksg = nullptr;
  ObIKSurrogateProcessor *surrogate_seg = nullptr;
  if (OB_ISNULL(letter_seg = OB_NEWx(ObIKLetterProcessor, &call_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_quan_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cnqsg = OB_NEWx(ObIKQuantifierProcessor, &call_alloc_, *dict_quan_, call_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(dict_main_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(cjksg = OB_NEWx(ObIKCJKProcessor, &call_alloc_, *dict_main_, call_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(surrogate_seg = OB_NEWx(ObIKSurrogateProcessor, &call_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(segmenters_.push_back(letter_seg))) {
  } else if (FALSE_IT(letter_seg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cnqsg))) {
  } else if (FALSE_IT(cnqsg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(cjksg))) {
  } else if (FALSE_IT(cjksg = nullptr)) {
  } else if (OB_FAIL(segmenters_.push_back(surrogate_seg))) {
  } else if (OB_FALSE_IT(surrogate_seg = nullptr)) {
  }
  // push back by order, quantifier is before cjk

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKLetterProcessor, &call_alloc_, letter_seg);
    OB_DELETEx(ObIKQuantifierProcessor, &call_alloc_, cnqsg);
    OB_DELETEx(ObIKCJKProcessor, &call_alloc_, cjksg);
    OB_DELETEx(ObIKSurrogateProcessor, &call_alloc_, surrogate_seg);
  }
  return ret;
}

void ObIKFTParser::reset()
{
  if (!OB_ISNULL(ctx_)) {
    ctx_->~TokenizeContext();
    call_alloc_.free(ctx_);
  }

  for (ObIIKProcessor *segmenter : segmenters_) {
    if (!OB_ISNULL(segmenter)) {
      segmenter->~ObIIKProcessor();
      call_alloc_.free(segmenter);
    }
  }
  segmenters_.clear();

  cache_main_.reset();
  cache_quan_.reset();
  cache_stop_.reset();

  if (!OB_ISNULL(dict_main_)) {
    dict_main_->~ObIFTDict();
    allocator_.free(dict_main_);
  }
  if (!OB_ISNULL(dict_quan_)) {
    dict_quan_->~ObIFTDict();
    allocator_.free(dict_quan_);
  }
  if (!OB_ISNULL(dict_stop_)) {
    dict_stop_->~ObIFTDict();
    allocator_.free(dict_stop_);
  }

  is_inited_ = false;
}

bool ObIKFTParser::should_read_newest_table() const { return false; }
int ObIKFTParser::build_dict_from_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &container,
                                        ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hub_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(dict = OB_NEWx(ObFTRangeDict, &allocator_, allocator_, &container, desc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(dict->init())) {
  }
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIFTDict, &allocator_, dict);
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase

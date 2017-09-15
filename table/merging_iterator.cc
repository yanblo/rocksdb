//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"

namespace rocksdb { 
void MergingIterator::SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
  ClearHeaps();
  for (auto& child : children_) {
    if (&child != current_) {
      child.Seek(key());
      if (child.Valid() && comparator_->Equal(key(), child.key())) {
        child.Next();
      }
    }
    if (child.Valid()) {
      minHeap_.push(&child);
    }
  }
  direction_ = kForward;
}

void MergingIterator::ClearHeaps() {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_.reset(new MergerMaxIterHeap(comparator_));
  }
}

MergingIterator* WrapToMergingIterator(InternalIterator* iter) {
  MergeIteratorBuilder builder(nullptr, nullptr, false);
  builder.AddIterator(iter);
  return builder.Finish();
}

MergingIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                    InternalIterator** list, int n,
                                    Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (arena == nullptr) {
    return new MergingIterator(cmp, list, n, false, prefix_seek_mode);
  } else {
    auto mem = arena->AllocateAligned(sizeof(MergingIterator));
    return new (mem) MergingIterator(cmp, list, n, true, prefix_seek_mode);
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode)
    : arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIterator));
  merge_iter =
      new (mem) MergingIterator(comparator, nullptr, 0, true, prefix_seek_mode);
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  merge_iter->AddIterator(iter);
}

MergingIterator* MergeIteratorBuilder::Finish() {
  MergingIterator* ret = merge_iter;
  merge_iter = nullptr;
  return ret;
}

}  // namespace rocksdb

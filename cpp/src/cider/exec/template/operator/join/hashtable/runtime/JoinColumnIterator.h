/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

//! Iterates over the rows of a JoinColumn across multiple fragments/chunks.
struct JoinColumnIterator {
  const JoinColumn* join_column;
  const JoinColumnTypeInfo* type_info;
  const struct JoinChunk* join_chunk_array;
  const int8_t* chunk_data;  // bool(chunk_data) tells if this iterator is valid
  size_t index_of_chunk;
  size_t index_inside_chunk;
  size_t index;
  size_t start;
  size_t step;

  FORCE_INLINE operator bool() const { return chunk_data; }

  FORCE_INLINE const int8_t* ptr() const {
    return &chunk_data[index_inside_chunk * join_column->elem_sz];
  }

  FORCE_INLINE int64_t getElementSwitch() const {
    switch (type_info->column_type) {
      case SmallDate:
        return SUFFIX(fixed_width_small_date_decode_noinline)(
            chunk_data,
            type_info->elem_sz,
            type_info->elem_sz == 4 ? NULL_INT : NULL_SMALLINT,
            type_info->elem_sz == 4 ? NULL_INT : NULL_SMALLINT,
            index_inside_chunk);
      case Signed:
        return SUFFIX(fixed_width_int_decode_noinline)(
            chunk_data, type_info->elem_sz, index_inside_chunk);
      case Unsigned:
        return SUFFIX(fixed_width_unsigned_decode_noinline)(
            chunk_data, type_info->elem_sz, index_inside_chunk);
      case Double:
        return SUFFIX(fixed_width_double_decode_noinline)(chunk_data, index_inside_chunk);
      default:
        CHECK(false);
        return 0;
    }
  }

  struct IndexedElement {
    size_t index;
    int64_t element;
  };  // struct IndexedElement

  FORCE_INLINE IndexedElement operator*() const { return {index, getElementSwitch()}; }

  FORCE_INLINE JoinColumnIterator& operator++() {
    index += step;
    index_inside_chunk += step;
    while (chunk_data &&
           index_inside_chunk >= join_chunk_array[index_of_chunk].num_elems) {
      index_inside_chunk -= join_chunk_array[index_of_chunk].num_elems;
      ++index_of_chunk;
      if (index_of_chunk < join_column->num_chunks) {
        chunk_data = join_chunk_array[index_of_chunk].col_buff;
      } else {
        chunk_data = nullptr;
      }
    }
    return *this;
  }

  JoinColumnIterator() : chunk_data(nullptr) {}

  JoinColumnIterator(const JoinColumn* join_column,
                     const JoinColumnTypeInfo* type_info,
                     size_t start,
                     size_t step)
      : join_column(join_column)
      , type_info(type_info)
      , join_chunk_array(
            reinterpret_cast<const struct JoinChunk*>(join_column->col_chunks_buff))
      , chunk_data(join_column->num_elems > 0 ? join_chunk_array->col_buff : nullptr)
      , index_of_chunk(0)
      , index_inside_chunk(0)
      , index(0)
      , start(start)
      , step(step) {
    // Stagger the index differently for each thread iterating over the column.
    auto temp = this->step;
    this->step = this->start;
    operator++();
    this->step = temp;
  }
};  // struct JoinColumnIterator

//! Helper class for viewing a JoinColumn and it's matching JoinColumnTypeInfo as a single
//! object.
struct JoinColumnTyped {
  // NOTE(sy): Someday we might want to merge JoinColumnTypeInfo into JoinColumn but
  // this class is a good enough solution for now until we have time to do more cleanup.
  const struct JoinColumn* join_column;
  const struct JoinColumnTypeInfo* type_info;

  JoinColumnIterator begin() { return JoinColumnIterator(join_column, type_info, 0, 1); }

  JoinColumnIterator end() { return JoinColumnIterator(); }

  struct Slice {
    JoinColumnTyped* join_column_typed;
    size_t start;
    size_t step;

    JoinColumnIterator begin() {
      return JoinColumnIterator(
          join_column_typed->join_column, join_column_typed->type_info, start, step);
    }

    JoinColumnIterator end() { return JoinColumnIterator(); }

  };  // struct Slice

  Slice slice(size_t start, size_t step) { return Slice{this, start, step}; }

};  // struct JoinColumnTyped

//! Iterates over the rows of a JoinColumnTuple across multiple fragments/chunks.
struct JoinColumnTupleIterator {
  // NOTE(sy): Someday we'd prefer to JIT compile this iterator, producing faster,
  // custom, code for each combination of column types encountered at runtime.

  size_t num_cols;
  JoinColumnIterator join_column_iterators[g_maximum_conditions_to_coalesce];

  // NOTE(sy): Are these multiple iterator instances (one per column) required when
  // we are always pointing to the same row in all N columns? Yes they are required,
  // if the chunk sizes can be different from column to column. I don't know if they
  // can or can't, so this code plays it safe for now.

  JoinColumnTupleIterator() : num_cols(0) {}

  JoinColumnTupleIterator(size_t num_cols,
                          const JoinColumn* join_column_per_key,
                          const JoinColumnTypeInfo* type_info_per_key,
                          size_t start,
                          size_t step)
      : num_cols(num_cols) {
    CHECK_LE(num_cols, g_maximum_conditions_to_coalesce);
    for (size_t i = 0; i < num_cols; ++i) {
      join_column_iterators[i] =
          JoinColumnIterator(&join_column_per_key[i],
                             type_info_per_key ? &type_info_per_key[i] : nullptr,
                             start,
                             step);
    }
  }

  FORCE_INLINE operator bool() const {
    for (size_t i = 0; i < num_cols; ++i) {
      if (join_column_iterators[i]) {
        return true;
        // If any column iterator is still valid, then the tuple is still valid.
      }
    }
    return false;
  }

  FORCE_INLINE JoinColumnTupleIterator& operator++() {
    for (size_t i = 0; i < num_cols; ++i) {
      ++join_column_iterators[i];
    }
    return *this;
  }

  FORCE_INLINE JoinColumnTupleIterator& operator*() { return *this; }
};  // struct JoinColumnTupleIterator

//! Helper class for viewing multiple JoinColumns and their matching JoinColumnTypeInfos
//! as a single object.
struct JoinColumnTuple {
  size_t num_cols;
  const JoinColumn* join_column_per_key;
  const JoinColumnTypeInfo* type_info_per_key;

  JoinColumnTuple()
      : num_cols(0), join_column_per_key(nullptr), type_info_per_key(nullptr) {}

  JoinColumnTuple(size_t num_cols,
                  const JoinColumn* join_column_per_key,
                  const JoinColumnTypeInfo* type_info_per_key)
      : num_cols(num_cols)
      , join_column_per_key(join_column_per_key)
      , type_info_per_key(type_info_per_key) {}

  JoinColumnTupleIterator begin() {
    return JoinColumnTupleIterator(
        num_cols, join_column_per_key, type_info_per_key, 0, 1);
  }

  JoinColumnTupleIterator end() { return JoinColumnTupleIterator(); }

  struct Slice {
    JoinColumnTuple* join_column_tuple;
    size_t start;
    size_t step;

    JoinColumnTupleIterator begin() {
      return JoinColumnTupleIterator(join_column_tuple->num_cols,
                                     join_column_tuple->join_column_per_key,
                                     join_column_tuple->type_info_per_key,
                                     start,
                                     step);
    }

    JoinColumnTupleIterator end() { return JoinColumnTupleIterator(); }

  };  // struct Slice

  Slice slice(size_t start, size_t step) { return Slice{this, start, step}; }

};  // struct JoinColumnTuple

#pragma once
#include <thirdparty/duckdb/third_party/pcg/pcg_random.hpp>

/// Fairly good thread-safe random number generator, but probably slow-down thread
/// creation a little.
extern thread_local pcg64 thread_local_rng;

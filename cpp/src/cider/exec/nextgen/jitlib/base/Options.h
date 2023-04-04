#ifndef JITLIB_BASE_OPTIONS_H
#define JITLIB_BASE_OPTIONS_H

namespace cider::jitlib {
// compilation config info
struct CompilationOptions {
  bool optimize_ir = true;
  bool aggressive_jit_compile = true;
  bool dump_ir = false;
  bool enable_vectorize = true;
  bool enable_avx2 = true;
  bool enable_avx512 = false;
};
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_OPTIONS_H

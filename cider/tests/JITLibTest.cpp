
#include "exec/nextgen/jitlib/JITLib.h"

#include <gtest/gtest.h>

#include "tests/TestHelpers.h"

using namespace jitlib;

class JITLibTests : public ::testing::Test {};

TEST_F(JITLibTests, BasicTest) {
  LLVMJITModule module("Test");

  using func_type = int32_t(*)(int32_t);
  LLVMJITFunction function = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func",
      .ret_type = JITFunctionParam{.type = INT32},
      .params_type = {JITFunctionParam{.name = "x", .type = INT32}},
  });
  {
    Value x = createVariable<INT32>(function, "x1", 123);
    createRet(function, x);
  }
  function.finish();

  module.finish();
  func_type ptr = reinterpret_cast<func_type>(module.getFunctionPtr(function));
  EXPECT_EQ(ptr(12), 123);
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  namespace po = boost::program_options;

  po::options_description desc("Options");

  logger::LogOptions log_options(argv[0]);
  log_options.max_files_ = 0;  // stderr only by default
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}

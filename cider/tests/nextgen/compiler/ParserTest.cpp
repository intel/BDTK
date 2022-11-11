
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "exec/nextgen/parsers/Parser.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "tests/utils/Utils.h"
#include "tests/TestHelpers.h"

class NextGenParserTest : public ::testing::Test {
 public:
  void executeTest(const std::string& sql) {
    auto json = RunIsthmus::processSql(sql, create_ddl_);
    ::substrait::Plan plan;
    google::protobuf::util::JsonStringToMessage(json, &plan);

    generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
    auto eu = substrait2eu.createRelAlgExecutionUnit();

    auto pipeline = cider::exec::nextgen::parsers::toOpPipeline(eu);
    EXPECT_EQ(pipeline.size(), 3);
  }

 private:
  std::string create_ddl_ =
      "CREATE TABLE test(a BIGINT NOT NULL, b BIGINT, c BIGINT);";
};

TEST_F(NextGenParserTest, ParserTest) {
  executeTest("select (a+b)*(b-c) from test where a > b or b < c");
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
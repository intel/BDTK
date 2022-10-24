
set(DEP_LIBS ${EXECUTE_TEST_LIBS} jitlib)

set(TEST_ARGS "--gtest_output=xml:../")

add_executable(JITLibTest JITLibTest.cpp)
target_link_libraries(JITLibTest ${DEP_LIBS})
add_test(JITLibTest JITLibTest ${TEST_ARGS})
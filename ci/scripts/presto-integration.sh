#!/bin/bash
# Copyright(c) 2022-2023 Intel Corporation.

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Make sure build-presto-package.sh and presto-bdtk-*.patch are in the same directory
pushd /workspace/github-workspace/presto/presto-native-execution
set -e

PRESTO_CPP_MODE=release
BDTK_BUILD_MODE=Release

rm -rf ./velox
rm -rf ./presto_cpp/main/lib
cp -r ./BDTK/cpp/thirdparty/velox .
mkdir -p ./presto_cpp/main/lib

sed -i 's|BDTK/src|BDTK/cpp/src|g' presto_cpp/main/PrestoServer.cpp
sed -i 's|BDTK/src|BDTK/cpp/src|g' presto_cpp/main/TaskResource.cpp

cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider-velox/src/libvelox_plugin.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider-velox/src/ciderTransformer/libcider_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider-velox/src/planTransformer/libvelox_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider-velox/src/substrait/libvelox_substrait_convertor.a ./presto_cpp/main/lib
cp -a ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/module/libcider.so* ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/processor/libcider_processor.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/plan/substrait/libcider_plan_substrait.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/thirdparty/velox/velox/substrait/libvelox_substrait_plan_converter.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/template/libQueryEngine.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/function/libcider_function.so ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/nextgen/jitlib/libjitlib.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/nextgen/libnextgen.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/plan/parser/libcider_plan_parser.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/exec/plan/lookup/libcider_func_lkup.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/thirdparty/SUBSTRAITCPP/src/SUBSTRAITCPP/substrait/common/libsubstrait_common.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/thirdparty/SUBSTRAITCPP/src/SUBSTRAITCPP/substrait/function/libsubstrait_function.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/thirdparty/SUBSTRAITCPP/src/SUBSTRAITCPP/substrait/type/libsubstrait_type.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/thirdparty/SUBSTRAITCPP/src/SUBSTRAITCPP/third_party/yaml-cpp/libyaml-cpp.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/util/libcider_util.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/function/libcider_runtime_function.so ./presto_cpp/main/lib

make -j ${CPU_COUNT:-`nproc`} PRESTO_ENABLE_PARQUET=ON VELOX_ENABLE_HDFS=ON ${PRESTO_CPP_MODE}
rm -rf ./_build/${PRESTO_CPP_MODE}/presto_cpp/function
mkdir -p ./_build/${PRESTO_CPP_MODE}/presto_cpp/function
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/function/RuntimeFunctions.bc ./_build/${PRESTO_CPP_MODE}/presto_cpp/function/
popd

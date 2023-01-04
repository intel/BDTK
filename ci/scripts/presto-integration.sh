#!/bin/bash
# Copyright (c) 2022 Intel Corporation.

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
cp -r ./BDTK/thirdparty/velox .
mkdir -p ./presto_cpp/main/lib

cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider-velox/src/libvelox_plugin.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider-velox/src/ciderTransformer/libcider_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider-velox/src/planTransformer/libvelox_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider-velox/src/substrait/libvelox_substrait_convertor.a ./presto_cpp/main/lib
cp -a ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/exec/module/libcider.so* ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/exec/processor/libcider_processor.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/exec/plan/substrait/libcider_plan_substrait.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/thirdparty/velox/velox/substrait/libvelox_substrait_plan_converter.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/exec/template/libQueryEngine.a ./presto_cpp/main/lib
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/function/libcider_function.a ./presto_cpp/main/lib

make -j ${CPU_COUNT:-`nproc`} PRESTO_ENABLE_PARQUET=ON VELOX_ENABLE_HDFS=ON ${PRESTO_CPP_MODE}
rm -rf ./_build/${PRESTO_CPP_MODE}/presto_cpp/function
mkdir -p ./_build/${PRESTO_CPP_MODE}/presto_cpp/function
cp ./BDTK/build-${BDTK_BUILD_MODE}/src/cider/function/RuntimeFunctions.bc ./_build/${PRESTO_CPP_MODE}/presto_cpp/function/
popd

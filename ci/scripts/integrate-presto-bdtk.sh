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
set -e

VELOX_PLUGIN_MODE=Debug
PRESTO_CPP_MODE=debug
if [ "$1" = "release" ] || [ "$1" = "Release" ] 
then 
    echo "Integration under Release mode"
    VELOX_PLUGIN_MODE=Release
    PRESTO_CPP_MODE=release
fi


git clone https://github.com/intel/BDTK.git ${WORKER_DIR}/BDTK
pushd ${WORKER_DIR}/BDTK
git submodule update --init --recursive
# run the build script
make ${PRESTO_CPP_MODE}
popd
pushd ${WORKER_DIR}
# copy the cider lib to presto_cpp
cp -r ${WORKER_DIR}/BDTK/thirdparty/velox/ .

sed -i 's/\"planTransformer\/PlanTransformer\.h\"/\"..\/planTransformer\/PlanTransformer\.h\"/' ${WORKER_DIR}/BDTK/cider-velox/src/ciderTransformer/CiderPlanTransformerFactory.h

rm -rf ${WORKER_DIR}/presto_cpp/main/lib
mkdir -p ${WORKER_DIR}/presto_cpp/main/lib


cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider-velox/src/libvelox_plugin.a ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider-velox/src/ciderTransformer/libcider_plan_transformer.a ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider-velox/src/planTransformer/libvelox_plan_transformer.a ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider-velox/src/substrait/libvelox_substrait_convertor.a ${WORKER_DIR}/presto_cpp/main/lib
cp -a ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider/exec/module/libcider.so* ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/thirdparty/velox/velox/substrait/libvelox_substrait_plan_converter.a ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider/exec/template/libQueryEngine.a ${WORKER_DIR}/presto_cpp/main/lib
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider/function/libcider_function.a ${WORKER_DIR}/presto_cpp/main/lib


make -j ${CPU_COUNT:-`nproc`} PRESTO_ENABLE_PARQUET=ON ${PRESTO_CPP_MODE}

rm -rf ${WORKER_DIR}/_build/${PRESTO_CPP_MODE}/presto_cpp/function
mkdir ${WORKER_DIR}/_build/${PRESTO_CPP_MODE}/presto_cpp/function
cp ${WORKER_DIR}/BDTK/build-${VELOX_PLUGIN_MODE}/cider/function/RuntimeFunctions.bc ${WORKER_DIR}/_build/${PRESTO_CPP_MODE}/presto_cpp/function/

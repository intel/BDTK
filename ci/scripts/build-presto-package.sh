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
cd "$(dirname "$0")"
git clone --recursive https://github.com/Intel-bigdata/presto.git
pushd presto
git checkout -b BDTK origin/BDTK
pushd presto-native-execution
git clone --recursive https://github.com/intel/BDTK.git
pushd BDTK  
make release
if [ $? -ne 0 ]; then
    echo "compile BDTK failed"
    exit
fi
popd

cp -r ./BDTK/thirdparty/velox .
mkdir -p ./presto_cpp/main/lib

cp ./BDTK/build-Release/cider-velox/src/libvelox_plugin.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/cider-velox/src/ciderTransformer/libcider_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/cider-velox/src/planTransformer/libvelox_plan_transformer.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/cider-velox/src/substrait/libvelox_substrait_convertor.a ./presto_cpp/main/lib
cp -a ./BDTK/build-Release/cider/exec/module/libcider.so* ./presto_cpp/main/lib
cp ./BDTK/build-Release/thirdparty/velox/velox/substrait/libvelox_substrait_plan_converter.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/cider/exec/template/libQueryEngine.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/cider/function/libcider_function.a ./presto_cpp/main/lib
cp ./BDTK/build-Release/thirdparty/velox/third_party/yaml-cpp/libyaml-cpp.a ./presto_cpp/main/lib

sed -i 's/\"planTransformer\/PlanTransformer\.h\"/\"..\/planTransformer\/PlanTransformer\.h\"/' ./BDTK/cider-velox/src/ciderTransformer/CiderPlanTransformerFactory.h
sed -i 's/\"velox-plugin\/cider-velox\/src\/CiderVeloxPluginCtx.h\"/\"BDTK\/cider-velox\/src\/CiderVeloxPluginCtx.h\"/' ./presto_cpp/main/TaskResource.cpp
sed -i 's/\"velox-plugin\/cider-velox\/src\/CiderVeloxPluginCtx.h\"/\"BDTK\/cider-velox\/src\/CiderVeloxPluginCtx.h\"/' ./presto_cpp/main/PrestoServer.cpp

make PRESTO_ENABLE_PARQUET=ON -j ${CPU_COUNT:-`nproc`} release
if [ $? -ne 0 ]; then
    echo "compile presto failed"
    exit
fi
mkdir -p ./_build/release/presto_cpp/function
cp ./BDTK/build-Release/cider/function/RuntimeFunctions.bc ./_build/release/presto_cpp/function/

popd
popd

# build package
package_name=Prestodb
rm -rf ${package_name} ${package_name}.tar.gz
mkdir -p ${package_name}/lib
mkdir -p ${package_name}/function
mkdir -p ${package_name}/bin
mkdir -p ${package_name}/archive
cp  ./presto/presto-native-execution/_build/release/presto_cpp/function/RuntimeFunctions.bc ./${package_name}/function

cp  ./presto/presto-native-execution/_build/release/presto_cpp/main/presto_server ./${package_name}/bin
cp  ./presto/presto-native-execution/presto_cpp/main/lib/libcider.so  ./${package_name}/lib
cp	/usr/local/lib/libantlr4-runtime.so.4.9.3	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libre2.so.5	./${package_name}/lib
cp	/lib/libprotobuf.so.32	./${package_name}/lib
cp	/usr/local/lib/libLLVM-9.so	./${package_name}/lib
cp	/usr/local/lib/libtbb.so.12	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libdouble-conversion.so.3	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libglog.so.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libgflags.so.2.2	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libsodium.so.23	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libssl.so.1.1	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libcrypto.so.1.1	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_context.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libevent-2.1.so.7	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libsnappy.so.1	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_regex.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_locale.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_log.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_filesystem.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_program_options.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libboost_thread.so.1.71.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libedit.so.2	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libunwind.so.8	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libicui18n.so.66	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libicuuc.so.66	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libbsd.so.0	./${package_name}/lib
cp	/lib/x86_64-linux-gnu/libicudata.so.66	./${package_name}/lib

tar -czvf ${package_name}.tar.gz ${package_name}

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib
# ./bin/presto_server -etc_dir=./etc


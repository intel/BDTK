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

make VELOX_ENABLE_HDFS=ON PRESTO_ENABLE_PARQUET=ON -j ${CPU_COUNT:-`nproc`} release
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
cp ./presto/presto-native-execution/_build/release/presto_cpp/function/RuntimeFunctions.bc ./${package_name}/function

DEPS_LIBRARY=(
    ./presto/presto-native-execution/presto_cpp/main/lib/libcider.so
    /usr/local/lib/libantlr4-runtime.so
    /lib/x86_64-linux-gnu/libre2.so
    /lib/libprotobuf.so
    /usr/local/lib/libLLVM-9.so
    /usr/local/lib/libtbb.so
    /usr/local/lib/libhdfs3.so
    /usr/lib/x86_64-linux-gnu/libgsasl.so
    /usr/lib/x86_64-linux-gnu/libntlm.so
    /lib/x86_64-linux-gnu/libdouble-conversion.so
    /lib/x86_64-linux-gnu/libglog.so
    /lib/x86_64-linux-gnu/libgflags.so
    /lib/x86_64-linux-gnu/libsodium.so
    /lib/x86_64-linux-gnu/libssl.so
    /lib/x86_64-linux-gnu/libcrypto.so
    /lib/x86_64-linux-gnu/libboost_context.so
    /lib/x86_64-linux-gnu/libevent-2.1.so
    /lib/x86_64-linux-gnu/libsnappy.so
    /lib/x86_64-linux-gnu/libboost_regex.so
    /lib/x86_64-linux-gnu/libboost_locale.so
    /lib/x86_64-linux-gnu/libboost_log.so
    /lib/x86_64-linux-gnu/libboost_filesystem.so
    /lib/x86_64-linux-gnu/libboost_program_options.so
    /lib/x86_64-linux-gnu/libboost_thread.so
    /lib/x86_64-linux-gnu/libedit.so
    /lib/x86_64-linux-gnu/libunwind.so
    /lib/x86_64-linux-gnu/libicui18n.so
    /lib/x86_64-linux-gnu/libicuuc.so
    /lib/x86_64-linux-gnu/libbsd.so
    /lib/x86_64-linux-gnu/libicudata.so
)
cp ./presto/presto-native-execution/_build/release/presto_cpp/main/presto_server ./${package_name}/bin

for lib_prefix in ${DEPS_LIBRARY[@]}
do
    cp -a ${lib_prefix}* ./${package_name}/lib
done

tar -czvf ${package_name}.tar.gz ${package_name}

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib
# ./bin/presto_server -etc_dir=./etc


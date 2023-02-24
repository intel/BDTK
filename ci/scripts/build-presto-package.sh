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
cd "$(dirname "$0")"
set -e

PRESTO_CPP_MODE=release     # -a

while getopts :a: options;
do
    case $options in
        a)
            PRESTO_CPP_MODE=${OPTARG}
        ;;
        ?)
            echo "Unknown parameter !"
            exit 1
        ;;
    esac
done

BDTK_BUILD_MODE=Release
if [ "$PRESTO_CPP_MODE" = "debug" ]
then
    BDTK_BUILD_MODE=Debug
fi

rm -rf presto
PATCH_NAME=presto-bdtk-67b3bf.patch
PRESTO_BDTK_COMMIT_ID=67b3bf5251f81131328dbd183685fb50e5a7ac2c

git clone https://github.com/prestodb/presto.git
pushd presto/presto-native-execution
git checkout -b WW43 ${PRESTO_BDTK_COMMIT_ID}
git apply ../../${PATCH_NAME}
git clone --recursive https://github.com/intel/BDTK.git
pushd BDTK
make  ${PRESTO_CPP_MODE}
popd

cp -r ./BDTK/cpp/thirdparty/velox .
mkdir -p ./presto_cpp/main/lib

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

make -j ${CPU_COUNT:-`nproc`} PRESTO_ENABLE_PARQUET=ON VELOX_ENABLE_HDFS=ON ${PRESTO_CPP_MODE}
mkdir -p ./_build/${PRESTO_CPP_MODE}/presto_cpp/function
cp ./BDTK/build-${BDTK_BUILD_MODE}/cpp/src/cider/function/RuntimeFunctions.bc ./_build/${PRESTO_CPP_MODE}/presto_cpp/function/
popd

# build package
package_name=Prestodb
rm -rf ${package_name} ${package_name}.tar.gz
mkdir -p ${package_name}/lib
mkdir -p ${package_name}/function
mkdir -p ${package_name}/bin
mkdir -p ${package_name}/conf
cp -r ./presto/presto-native-execution/BDTK/cpp/src/cider/function/extensions ./${package_name}/conf
cp -r ./presto/presto-native-execution/BDTK/cpp/src/cider/function/internals ./${package_name}/conf
cp -a ./presto/presto-native-execution/_build/${PRESTO_CPP_MODE}/presto_cpp/function/RuntimeFunctions.bc ./${package_name}/function
cp -a ./presto/presto-native-execution/_build/${PRESTO_CPP_MODE}/presto_cpp/main/presto_server ./${package_name}/bin
cp -r ./presto/presto-native-execution/BDTK/cpp/src/cider-velox/test/e-2-e ./${package_name}/

DEPS_LIBRARY=( 
    ./presto/presto-native-execution/presto_cpp/main/lib/libcider.so*
    /usr/local/lib/libantlr4-runtime.so.4.9.3
    /usr/local/lib/libLLVM-9.so
    /usr/local/lib/libtbb.so.12
    /usr/local/lib/libhdfs3.so.1
    /lib/x86_64-linux-gnu/libglog.so.0
    /lib/x86_64-linux-gnu/libgflags.so.2.2
    /lib/x86_64-linux-gnu/libdouble-conversion.so.3
    /lib/x86_64-linux-gnu/libsodium.so.23
    /lib/x86_64-linux-gnu/libssl.so.1.1
    /lib/x86_64-linux-gnu/libcrypto.so.1.1
    /lib/x86_64-linux-gnu/libboost_context.so.1.71.0
    /lib/x86_64-linux-gnu/libevent-2.1.so.7
    /lib/x86_64-linux-gnu/libsnappy.so.1
    /lib/x86_64-linux-gnu/libboost_regex.so.1.71.0
    /lib/libprotobuf.so.32
    /lib/x86_64-linux-gnu/libre2.so.5   
    /usr/lib/x86_64-linux-gnu/libdouble-conversion.so.3.1
    /usr/lib/x86_64-linux-gnu/libevent-2.1.so.7.0.0
    /usr/lib/x86_64-linux-gnu/libgflags.so.2.2.2
    /usr/lib/x86_64-linux-gnu/libglog.so.0.0.0
    /usr/local/lib/libhdfs3.so.2.2.31
    /usr/lib/libprotobuf.so.32.0.4
    /usr/lib/x86_64-linux-gnu/libre2.so.5.0.0
    /usr/lib/x86_64-linux-gnu/libsnappy.so.1.1.8
    /usr/lib/x86_64-linux-gnu/libsodium.so.23.3.0
    /usr/local/lib/libtbb.so.12.3   
    /lib/x86_64-linux-gnu/libboost_filesystem.so.1.71.0
    /lib/x86_64-linux-gnu/libboost_locale.so.1.71.0
    /lib/x86_64-linux-gnu/libboost_log.so.1.71.0
    /lib/x86_64-linux-gnu/libboost_program_options.so.1.71.0
    /lib/x86_64-linux-gnu/libboost_thread.so.1.71.0
    /lib/x86_64-linux-gnu/libedit.so.2
    /usr/lib/x86_64-linux-gnu/libedit.so.2.0.63
    /lib/x86_64-linux-gnu/libgsasl.so.7
    /usr/lib/x86_64-linux-gnu/libgsasl.so.7.9.7
    /lib/x86_64-linux-gnu/libunwind.so.8
    /usr/lib/x86_64-linux-gnu/libunwind.so.8.0.1
    /lib/x86_64-linux-gnu/libbsd.so.0
    /usr/lib/x86_64-linux-gnu/libbsd.so.0.10.0
    /lib/x86_64-linux-gnu/libidn.so.11
    /usr/lib/x86_64-linux-gnu/libidn.so.11.6.16
    /lib/x86_64-linux-gnu/libntlm.so.0
    /usr/lib/x86_64-linux-gnu/libntlm.so.0.0.20
    /lib/x86_64-linux-gnu/libgssapi_krb5.so.2
    /lib/x86_64-linux-gnu/libgssapi_krb5.so.2.2
    /usr/lib/x86_64-linux-gnu/libkrb5.so.3
    /usr/lib/x86_64-linux-gnu/libkrb5.so.3.3
    /usr/lib/x86_64-linux-gnu/libk5crypto.so.3
    /usr/lib/x86_64-linux-gnu/libk5crypto.so.3.1
    /usr/lib/x86_64-linux-gnu/libkrb5support.so.0
    /usr/lib/x86_64-linux-gnu/libkrb5support.so.0.1
    /usr/lib/x86_64-linux-gnu/libkeyutils.so.1
    /usr/lib/x86_64-linux-gnu/libkeyutils.so.1.8    
    /usr/lib/x86_64-linux-gnu/libxml2.so.2
    /usr/lib/x86_64-linux-gnu/libxml2.so.2.9.10
    /usr/lib/x86_64-linux-gnu/libicui18n.so.66
    /usr/lib/x86_64-linux-gnu/libicui18n.so.66.1
    /usr/lib/x86_64-linux-gnu/libicuuc.so.66
    /usr/lib/x86_64-linux-gnu/libicuuc.so.66.1
    /usr/lib/x86_64-linux-gnu/libicudata.so.66
    /usr/lib/x86_64-linux-gnu/libicudata.so.66.1
)

for lib_prefix in ${DEPS_LIBRARY[@]}
do
    cp -a ${lib_prefix} ./${package_name}/lib
done

tar -czvf ${package_name}.tar.gz ${package_name}
rm -rf Prestodb

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib
# ./bin/presto_server -etc_dir=./etc

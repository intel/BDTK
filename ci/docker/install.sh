#!/bin/bash
# Copyright (c) 2022 Intel Corporation.
# Copyright (c) Omnisci, Inc. and its affiliates.
# Copyright (c) Facebook, Inc. and its affiliates.
#
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
set -x

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PREFIX=/usr/local/
export CMAKE_PREFIX_PATH=$PREFIX
export CMAKE_INSTALL_PREFIX=$PREFIX

COMPILER_FLAGS="-mavx2 -mfma -mavx -mf16c -mlzcnt -std=c++17"
LLVM_VERSION=9.0.1
TBB_VERSION=2021.3.0
THRIFT_VERSION=0.15.0
FB_OS_VERSION=v2022.07.11.00
HTTP_DEPS="https://dependencies.mapd.com/thirdparty"

BUILD_DIR=_thirdparty_build
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

function prompt {
  (
    while true; do
      local input="${PROMPT_ALWAYS_RESPOND:-}"
      echo -n "$(tput bold)$* [Y, n]$(tput sgr0) "
      [[ -z "${input}" ]] && read input
      if [[ "${input}" == "Y" || "${input}" == "y" || "${input}" == "" ]]; then
        return 0
      elif [[ "${input}" == "N" || "${input}" == "n" ]]; then
        return 1
      fi
    done
  ) 2> /dev/null
}

function download() {
  wget --continue "$1"
}

function extract() {
  tar xvf "$1"
}

function wget_and_untar {
  local URL=$1
  local DIR=$2
  mkdir -p "${DIR}"
  wget -q --max-redirect 3 -O - "${URL}" | tar -xz -C "${DIR}" --strip-components=1
}

# github_checkout $REPO $VERSION clones or re-uses an existing clone of the
# specified repo, checking out the requested version.
function github_checkout {
  local REPO=$1
  local VERSION=$2
  local DIRNAME=$(basename "$1")

  if [ -z "${DIRNAME}" ]; then
    echo "Failed to get repo name from $1"
    exit 1
  fi
  if [ -d "${DIRNAME}" ] && prompt "${DIRNAME} already exists. Delete?"; then
    rm -rf "${DIRNAME}"
  fi
  if [ ! -d "${DIRNAME}" ]; then
    git clone -q "https://github.com/${REPO}.git"
  fi
  pushd "${DIRNAME}"
  git fetch -q
  git checkout "${VERSION}"
  popd
}

function cmake_install {
  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"

  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_CXX_FLAGS="${COMPILER_FLAGS}" \
    -DBUILD_TESTING=OFF \
    "$@"
  sudo ninja -C "${BINARY_DIR}" install
}

function install_boost {
  rm -rf boost
  wget_and_untar https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.gz boost
  pushd boost
  ./bootstrap.sh --prefix=$PREFIX
  sudo ./b2 "-j$(nproc)" -d0 install threading=multi
  popd
}

function install_glog {
  rm -rf glog
  wget_and_untar https://github.com/google/glog/archive/v0.4.0.tar.gz glog
  pushd glog
  cmake_install -DBUILD_SHARED_LIBS=ON
  popd
}

function install_gflags {
  rm -rf gflags
  wget_and_untar https://github.com/gflags/gflags/archive/v2.2.2.tar.gz gflags
  pushd gflags
  cmake_install -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON
  popd
}

function install_protobuf {
  PROTOBUF_VERS=21.2
  rm -rf protobuf
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERS}/protobuf-all-${PROTOBUF_VERS}.tar.gz protobuf
  pushd protobuf
  cmake_install
  popd
}

function install_fmt {
  github_checkout fmtlib/fmt 8.0.0
  pushd fmt
  cmake_install -DFMT_TEST=OFF
  popd
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  pushd folly
  cmake_install -DBUILD_TESTS=OFF
  popd
}

function install_llvm() {
  LLVM_BUILD_DYLIB=true
  VERS=${LLVM_VERSION}
  download ${HTTP_DEPS}/llvm/$VERS/llvm-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/clang-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/compiler-rt-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/lldb-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/lld-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/libcxx-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/libcxxabi-$VERS.src.tar.xz
  download ${HTTP_DEPS}/llvm/$VERS/clang-tools-extra-$VERS.src.tar.xz
  rm -rf llvm-$VERS.src
  extract llvm-$VERS.src.tar.xz
  extract clang-$VERS.src.tar.xz
  extract compiler-rt-$VERS.src.tar.xz
  extract lld-$VERS.src.tar.xz
  extract lldb-$VERS.src.tar.xz
  extract libcxx-$VERS.src.tar.xz
  extract libcxxabi-$VERS.src.tar.xz
  extract clang-tools-extra-$VERS.src.tar.xz
  mv clang-$VERS.src llvm-$VERS.src/tools/clang
  mv compiler-rt-$VERS.src llvm-$VERS.src/projects/compiler-rt
  mv lld-$VERS.src llvm-$VERS.src/tools/lld
  mv lldb-$VERS.src llvm-$VERS.src/tools/lldb
  mv libcxx-$VERS.src llvm-$VERS.src/projects/libcxx
  mv libcxxabi-$VERS.src llvm-$VERS.src/projects/libcxxabi
  mkdir -p llvm-$VERS.src/tools/clang/tools
  mv clang-tools-extra-$VERS.src llvm-$VERS.src/tools/clang/tools/extra

  # Patch llvm 9 for glibc 2.31+ support
  # from: https://bugs.gentoo.org/708430
  pushd llvm-$VERS.src/projects/
  sudo patch -p0 < $SCRIPTS_DIR/llvm-9-glibc-2.31-708430-remove-cyclades.patch
  popd

  rm -rf build.llvm-$VERS
  mkdir build.llvm-$VERS
  pushd build.llvm-$VERS

  LLVM_SHARED=""
  if [ "$LLVM_BUILD_DYLIB" = "true" ]; then
    LLVM_SHARED="-DLLVM_BUILD_LLVM_DYLIB=ON -DLLVM_LINK_LLVM_DYLIB=ON"
  fi

  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$PREFIX -DLLVM_ENABLE_RTTI=on -DLLVM_USE_INTEL_JITEVENTS=on $LLVM_SHARED ../llvm-$VERS.src
  make -j ${nproc}
  sudo make install
  popd
}

function install_tbb() {
  download https://github.com/oneapi-src/oneTBB/archive/v${TBB_VERSION}.tar.gz
  extract v${TBB_VERSION}.tar.gz
  pushd oneTBB-${TBB_VERSION}
  mkdir build
  pushd build
  if [ "$1" == "static" ]; then
    cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DTBB_TEST=off -DBUILD_SHARED_LIBS=off \
      ${TBB_TSAN} \
      ..
  else
    cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PREFIX \
      -DTBB_TEST=off \
      -DBUILD_SHARED_LIBS=on \
      ${TBB_TSAN} \
      ..
  fi
  make -j ${nproc}
  sudo make install
  popd
  popd
}

function install_thrift() {
  download ${HTTP_DEPS}/thrift-$THRIFT_VERSION.tar.gz
  extract thrift-$THRIFT_VERSION.tar.gz
  pushd thrift-$THRIFT_VERSION
  THRIFT_CFLAGS="-fPIC"
  THRIFT_CXXFLAGS="-fPIC"
  BOOST_LIBDIR="--with-boost-libdir=$PREFIX/lib"
  CFLAGS="$THRIFT_CFLAGS" CXXFLAGS="$THRIFT_CXXFLAGS" JAVA_PREFIX=$PREFIX/lib ./configure \
      --prefix=$PREFIX \
      --enable-libs=off \
      --with-cpp \
      --without-go \
      --without-python \
      $BOOST_LIBDIR
  make -j ${nproc}
  sudo make install
  popd
}

function install_fizz {
  github_checkout facebookincubator/fizz "${FB_OS_VERSION}"
  pushd fizz
  cmake_install -DBUILD_TESTS=OFF -S fizz
  popd
}

function install_wangle {
  github_checkout facebook/wangle "${FB_OS_VERSION}"
  pushd wangle
  cmake_install -DBUILD_TESTS=OFF -S wangle
  popd
}

function install_fbthrift {
  github_checkout facebook/fbthrift "${FB_OS_VERSION}"
  pushd fbthrift
  cmake_install -DBUILD_TESTS=OFF
  popd
}

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  pushd proxygen
  cmake_install -DBUILD_TESTS=OFF
  popd
}

function install_antlr4 {
  if [ -d "antlr4-cpp-runtime-4.9.3-source" ]; then
    rm -rf antlr4-cpp-runtime-4.9.3-source
  fi
  wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip -O antlr4-cpp-runtime-4.9.3-source.zip
  mkdir antlr4-cpp-runtime-4.9.3-source
  pushd antlr4-cpp-runtime-4.9.3-source
  unzip ../antlr4-cpp-runtime-4.9.3-source.zip
  cmake_install -DCMAKE_INSTALL_PREFIX="$PREFIX"
  popd
}

function install_bdtk_deps {
  install_boost
  install_gflags
  install_glog
  install_protobuf
  install_tbb
  install_thrift
  install_llvm
}

function install_velox_deps {
  install_fmt
  install_folly
}

function install_presto_deps {
  install_fizz
  install_wangle
  install_fbthrift
  install_proxygen
  install_antlr4
}

install_bdtk_deps
install_velox_deps
install_presto_deps

rm -rf "${BUILD_DIR}"

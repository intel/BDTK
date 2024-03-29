#!/bin/bash
# Copyright(c) 2022-2023 Intel Corporation.
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is modified based on velox/scripts/set-updapters.sh from Meta (https://github.com/facebookincubator/velox)
SCRIPTDIR=$(cd $(dirname $0); pwd)
source $SCRIPTDIR/../../thirdparty/velox/scripts/setup-helper-functions.sh
# Propagate errors and improve debugging.
set -eufx -o pipefail

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}

function install_libhdfs3 {
  github_checkout apache/hawq master
  cd $DEPENDENCY_DIR/hawq/depends/libhdfs3
  if [[ "$OSTYPE" == darwin* ]]; then
     sed -i '' -e "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
     sed -i '' -e "s/dumpversion/dumpfullversion/" ./CMakeLists.txt
  fi

  if [[ "$OSTYPE" == linux-gnu* ]]; then
    sed -i "/FIND_PACKAGE(GoogleTest REQUIRED)/d" ./CMakeLists.txt
    sed -i "s/dumpversion/dumpfullversion/" ./CMake/Platform.cmake
  fi
  

  local NAME=$(basename "$(pwd)")
  local BINARY_DIR=_build
  if [ -d "${BINARY_DIR}" ] && prompt "Do you want to rebuild ${NAME}?"; then
    rm -rf "${BINARY_DIR}"
  fi
  mkdir -p "${BINARY_DIR}"
  CPU_TARGET="${CPU_TARGET:-avx}"
  COMPILER_FLAGS=$(get_cxx_flags $CPU_TARGET)

  # CMAKE_POSITION_INDEPENDENT_CODE is required so that Velox can be built into dynamic libraries \
  cmake -Wno-dev -B"${BINARY_DIR}" \
    -GNinja \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    "${INSTALL_PREFIX+-DCMAKE_PREFIX_PATH=}${INSTALL_PREFIX-}" \
    "${INSTALL_PREFIX+-DCMAKE_INSTALL_PREFIX=}${INSTALL_PREFIX-}" \
    -DCMAKE_CXX_FLAGS="$COMPILER_FLAGS" \
    -DBUILD_TESTING=OFF \
    -DKERBEROS_INCLUDE_DIRS=${KERBEROS_INCLUDE_DIRS} \
    "$@"
  ninja -C "${BINARY_DIR}" install
}

function install_protobuf {
  rm -rf protobuf-all-21.4.tar.gz
  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz
  tar -xzf protobuf-all-21.4.tar.gz --no-same-owner
  cd protobuf-21.4
  ./configure --prefix=/usr
  make "-j$(nproc)"
  make install
  ldconfig
}

function install_kerberos {
  rm -rf krb5-1.20.tar.gz
  wget http://web.mit.edu/kerberos/dist/krb5/1.20/krb5-1.20.tar.gz
  tar -xzf krb5-1.20.tar.gz --no-same-owner
  cd krb5-1.20
  cp ./src/include/krb5/krb5.hin ./src/include/krb5/krb5.h
  export KERBEROS_INCLUDE_DIRS=$(pwd)/src/include
}

DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
cd "${DEPENDENCY_DIR}" || exit
# aws-sdk-cpp missing dependencies

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
   # /etc/os-release is a standard way to query various distribution
   # information and is available everywhere
   LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
   VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})
   if [[ "$LINUX_DISTRIBUTION" == "ubuntu" ]]; then
      # support for Ubuntu 20.04
      if [[ "$VERSION_ID" == "20.04" ]]; then
        apt install -y --no-install-recommends libxml2-dev libgsasl7-dev uuid-dev
      else
        apt install -y --no-install-recommends libxml2-dev libgsasl-dev uuid-dev
      fi
   else # Assume Fedora/CentOS
      yum -y install libxml2-devel libgsasl-devel libuuid-devel
   fi
fi

if [[ "$OSTYPE" == darwin* ]]; then
   brew install libxml2 gsasl
fi

install_protobuf
install_kerberos
install_libhdfs3

_ret=$?
if [ $_ret -eq 0 ] ; then
   echo "All deps for Velox adapters installed!"
fi
#!/usr/bin/env bash
# Copyright(c) 2022-2023 Intel Corporation.
# Copyright (c) Omnisci, Inc. and its affiliates.
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

# Parse inputs
TSAN=false
COMPRESS=false
NOCUDA=false

while (( $# )); do
  case "$1" in
    --compress)
      COMPRESS=true
      ;;
    --tsan)
      TSAN=true
      ;;
    --nocuda)
      NOCUDA=true
      ;;
    *)
      break
      ;;
  esac
  shift
done

HTTP_DEPS="https://dependencies.mapd.com/thirdparty"

SUFFIX=${SUFFIX:=$(date +%Y%m%d)}
PREFIX=/usr/local

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPTS_DIR/common-functions.sh

# Establish distro
source /etc/os-release
if [ "$ID" == "ubuntu" ] ; then
  PACKAGER="apt -y"
  if [ "$VERSION_ID" != "20.04" ] && [ "$VERSION_ID" != "19.10" ] && [ "$VERSION_ID" != "19.04" ] && [ "$VERSION_ID" != "18.04" ]; then
    echo "Ubuntu 20.04, 19.10, 19.04, and 18.04 are the only debian-based releases supported by this script"
    exit 1
  fi
else
  echo "Only Ubuntu is supported by this script"
  exit 1
fi

DEBIAN_FRONTEND=noninteractive sudo apt update

# required for gcc-9 on Ubuntu 18.04
if [ "$VERSION_ID" == "18.04" ]; then
  DEBIAN_FRONTEND=noninteractive sudo apt install -y software-properties-common
  DEBIAN_FRONTEND=noninteractive sudo add-apt-repository ppa:ubuntu-toolchain-r/test
fi

DEBIAN_FRONTEND=noninteractive sudo apt install -y \
    software-properties-common \
    build-essential \
    cmake \
    ccache \
    git \
    wget \
    curl \
    gcc \
    libboost-all-dev \
    libgoogle-glog-dev \
    libevent-dev \
    libcurl4-openssl-dev \
    libedit-dev \
    unzip \
    python-dev \
    python-yaml \
    swig \
    pkg-config \
    libtool

# Needed to find sqlite3, xmltooling, and xml_security_c
export PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:$PKG_CONFIG_PATH
export PATH=$PREFIX/bin:$PATH

install_ninja

# llvm
LLVM_BUILD_DYLIB=true
install_llvm

# install AWS core and s3 sdk
install_awscpp -j $(nproc)

VERS=0.16.0
wget --continue -O thrift-$VERS.tar.gz https://github.com/apache/thrift/archive/refs/tags/v$VERS.tar.gz
tar xvf thrift-$VERS.tar.gz
pushd thrift-$VERS
./bootstrap.sh
CFLAGS="-fPIC" CXXFLAGS="-fPIC" JAVA_PREFIX=$PREFIX/lib ./configure \
    --with-lua=no \
    --with-python=no \
    --with-php=no \
    --with-ruby=no \
    --with-qt4=no \
    --with-qt5=no \
    --with-java=no \
    --with-go=no \
    --prefix=$PREFIX
make -j $(nproc)
make install
popd

install_folly

# TBB
install_tbb

# Apache Arrow (see common-functions.sh)
ARROW_BOOST_USE_SHARED="ON"
install_arrow

cat > $PREFIX/mapd-deps.sh <<EOF
PREFIX=$PREFIX

LD_LIBRARY_PATH=/usr/local/cuda/lib64:\$LD_LIBRARY_PATH
LD_LIBRARY_PATH=\$PREFIX/lib:\$LD_LIBRARY_PATH
LD_LIBRARY_PATH=\$PREFIX/lib64:\$LD_LIBRARY_PATH

PATH=/usr/local/cuda/bin:\$PATH
PATH=\$PREFIX/bin:\$PATH

export LD_LIBRARY_PATH PATH
EOF

echo
echo "Done. Be sure to source the 'mapd-deps.sh' file to pick up the required environment variables:"
echo "    source $PREFIX/mapd-deps.sh"

if [ "$COMPRESS" = "true" ] ; then
    if [ "$TSAN" = "false" ]; then
      TARBALL_TSAN=""
    elif [ "$TSAN" = "true" ]; then
      TARBALL_TSAN="tsan-"
    fi
    tar acvf mapd-deps-ubuntu-${VERSION_ID}-${TARBALL_TSAN}${SUFFIX}.tar.xz -C ${PREFIX} .
fi

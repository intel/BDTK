#!/usr/bin/env bash
# Copyright (c) 2022 Intel Corporation.
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

#
# build-image.sh <OS> - prepares a Docker image with <OS>-base to the Dockerfile

set -e

function usage {
echo "Usage:"
echo "    build-image.sh <OS-VER>"
echo "where <OS-VER>, for example, can be 'ubuntu-20.04', provided" \
     "a Dockerfile named 'Dockerfile.ubuntu-20.04' exists in the" \
     "current directory."
}

if [[ -z "$1" ]]; then
  usage
  exit 1
fi

if [[ ! -f "Dockerfile.$1" ]]; then
  echo "ERROR: wrong argument."
  usage
  exit 1
fi

docker build -t bdtk/$1 \
  --build-arg HttpProxyHost=$proxy_host \
  --build-arg HttpsProxyHost=$proxy_host \
  --build-arg HttpProxyPort=$proxy_port \
  --build-arg HttpsProxyPort=$proxy_port \
  -f Dockerfile.$1 .

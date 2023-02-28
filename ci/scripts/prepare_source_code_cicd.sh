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


PRESTO_LOCAL_PATH=/workspace/github-workspace/presto
rm -rf ${PRESTO_LOCAL_PATH}
PATCH_NAME=presto-bdtk-67b3bf.patch
PATCH_PATH=$(pwd)/ci/scripts/${PATCH_NAME}
PRESTO_BDTK_COMMIT_ID=67b3bf5251f81131328dbd183685fb50e5a7ac2c

git clone https://github.com/prestodb/presto.git ${PRESTO_LOCAL_PATH}
pushd ${PRESTO_LOCAL_PATH}
git checkout -b cider ${PRESTO_BDTK_COMMIT_ID}
git apply ${PATCH_PATH}
popd


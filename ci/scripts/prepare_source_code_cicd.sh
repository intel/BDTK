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

CODE_BASE_PATH=/workspace/code
PRESTO_LOCAL_PATH=/workspace/github-workspace/presto
VELOX_COMMIT_ID=`git submodule status -- cpp/thirdparty/velox | cut -d' ' -f2`
echo "velox commit id: ${VELOX_COMMIT_ID}"

pushd ${CODE_BASE_PATH}/velox
git pull --rebase
VELOX_BRANCH_NAME=`git branch -r --contains ${VELOX_COMMIT_ID} | cut -d' ' -f3`
echo "branch name: ${VELOX_BRANCH_NAME}"
popd

PRESTO_LOCAL_BRANCH_NAME=`echo ${VELOX_BRANCH_NAME} | cut -d '/' -f2`
if [ -d $PRESTO_LOCAL_PATH ]; then
    rm -rf $PRESTO_LOCAL_PATH
fi
git clone -b $PRESTO_LOCAL_BRANCH_NAME https://github.com/intel-innersource/frameworks.ai.modular-sql.presto.git $PRESTO_LOCAL_PATH
mkdir $PRESTO_LOCAL_PATH/presto-native-execution/BDTK

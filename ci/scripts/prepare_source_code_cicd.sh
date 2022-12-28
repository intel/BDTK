#!/bin/bash
set -e

PRESTO_LOCAL_PATH=/workspace/github-workspace/presto
VELOX_COMMIT_ID=`git submodule status -- thirdparty/velox | cut -d' ' -f2`
echo "velox commit id: ${VELOX_COMMIT_ID}"

pushd thirdparty/velox
VELOX_BRANCH_NAME=`git branch -r --contains ${VELOX_COMMIT_ID} | cut -d' ' -f3`
echo "branch name: ${VELOX_BRANCH_NAME}"
popd
PRESTO_LOCAL_BRANCH_NAME=`echo ${VELOX_BRANCH_NAME} | cut -d '/' -f2`
rm -rf $PRESTO_LOCAL_PATH
git clone -b $PRESTO_LOCAL_BRANCH_NAME https://github.com/intel-innersource/frameworks.ai.modular-sql.presto.git $PRESTO_LOCAL_PATH
mkdir $PRESTO_LOCAL_PATH/presto-native-execution/BDTK

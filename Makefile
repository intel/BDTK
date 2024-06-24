# Copyright(c) 2022-2023 Intel Corporation.
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

.PHONY: all clean build debug release \
		test-cider test-cider-velox test-debug test-release

BUILD_TYPE := Release

ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
CPP_SOURCE_DIR := ${ROOT_DIR}/cpp
CPP_BUILD_DIR := ${ROOT_DIR}/build-${BUILD_TYPE}/cpp

all: release

format-fix:
	ci/scripts/check.py format branch --fix

format-check:
	clang-format --version
	ci/scripts/check.py format branch

header-fix:
	ci/scripts/check.py header branch --fix

header-check:
	ci/scripts/check.py header branch

lint:
	ci/scripts/run_cpplint.py                          \
		--cpplint_binary ci/scripts/cpplint.py         \
		--exclude_globs ci/scripts/lint_exclusions.txt \
		--source_dir ci/scripts/lint_inclusions.txt

clean:
	@rm -rf build-*

build-common:
	@sed -i "s/COMMAND protoc --proto_path \$${CMAKE_SOURCE_DIR}\/ --cpp_out \$${CMAKE_SOURCE_DIR}/COMMAND protoc --proto_path \$${proto_directory}\/ --cpp_out \$${PROTO_OUTPUT_DIR}/g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/CMakeLists.txt
	@sed -i "s/velox\/substrait\/proto\///g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto/substrait/algebra.proto
	@sed -i "s/velox\/substrait\/proto\///g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto/substrait/function.proto
	@sed -i "s/velox\/substrait\/proto\///g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto/substrait/parameterized_types.proto
	@sed -i "s/velox\/substrait\/proto\///g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto/substrait/plan.proto
	@sed -i "s/velox\/substrait\/proto\///g" ${CPP_SOURCE_DIR}/thirdparty/velox/velox/substrait/proto/substrait/type_expressions.proto
	# @sed -i 's|"https://.*arrow.*tar.gz"|"https://github.com/apache/arrow/archive/refs/tags/apache-arrow-8.0.0.tar.gz"|g' ${CPP_SOURCE_DIR}/thirdparty/velox/third_party/CMakeLists.txt

	@mkdir -p ${CPP_BUILD_DIR}
	@cd ${CPP_BUILD_DIR} && \
	cmake -Wno-dev \
		  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
		  -DENABLE_FOLLY=OFF \
		  -DENABLE_JIT_DEBUG=OFF \
		  -DENABLE_PERF_JIT_LISTENER=OFF\
		  -DENABLE_INTEL_JIT_LISTENER=OFF \
		  -DPREFER_STATIC_LIBS=OFF \
		  -DENABLE_ASN=OFF \
		  -DCIDER_ENABLE_AVX512=OFF\
		  -DVELOX_ENABLE_SUBSTRAIT=ON \
		  -DVELOX_ENABLE_PARQUET=ON \
		  ${EXTRA_OPTIONS} \
		  $(FORCE_COLOR) \
		  ${CPP_SOURCE_DIR}

build:
	VERBOSE=1 cmake --build ${CPP_BUILD_DIR} -j $${CPU_COUNT:-`nproc`} \
		|| cmake --build ${CPP_BUILD_DIR}
	@mkdir -p ${CPP_BUILD_DIR}/src/cider-velox/function/ \
		&& cp -r ${CPP_BUILD_DIR}/src/cider/function/*.bc ${CPP_BUILD_DIR}/src/cider-velox/function/
	@mkdir -p ${CPP_BUILD_DIR}/src/cider-velox/benchmark/function/ \
		&& cp -r ${CPP_BUILD_DIR}/src/cider/function/*.bc ${CPP_BUILD_DIR}/src/cider-velox/benchmark/function/

icl:
	@mkdir -p ${CPP_BUILD_DIR}
	@cd ${CPP_BUILD_DIR} && \
	cmake -Wno-dev \
		  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
		  -DBDTK_ENABLE_ICL=ON \
		  -DBDTK_ENABLE_CIDER=OFF \
		  $(FORCE_COLOR) \
			${CPP_SOURCE_DIR}
	VERBOSE=1 cmake --build ${CPP_BUILD_DIR} -j $${CPU_COUNT:-`nproc`} || \
	cmake --build ${CPP_BUILD_DIR}

icl-qat:
	@mkdir -p ${CPP_BUILD_DIR}
	@cd ${CPP_BUILD_DIR} && \
	cmake -Wno-dev \
		  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
		  -DBDTK_ENABLE_ICL=ON \
		  -DICL_WITH_QAT=ON \
		  -DBDTK_ENABLE_CIDER=OFF \
		  $(FORCE_COLOR) \
			${CPP_SOURCE_DIR}
	VERBOSE=1 cmake --build ${CPP_BUILD_DIR} -j $${CPU_COUNT:-`nproc`} || \
	cmake --build ${CPP_BUILD_DIR}

debug:
	@$(MAKE) build-common BUILD_TYPE=Debug
	@$(MAKE) build BUILD_TYPE=Debug

reldeb:
	@$(MAKE) build-common BUILD_TYPE=RelWithDebInfo
	@$(MAKE) build BUILD_TYPE=RelWithDebInfo

release:
	@$(MAKE) build-common BUILD_TYPE=Release
	@$(MAKE) build BUILD_TYPE=Release

benchmark:
	@$(MAKE) build-common BUILD_TYPE=Release \
		EXTRA_OPTIONS="-DENABLE_BENCHMARK=ON -DOPTIMIZE_FOR_NATIVE=OFF"
	@$(MAKE) build BUILD_TYPE=Release

test-cider:
	@cd ${CPP_BUILD_DIR}/src/cider/tests && \
	ctest -j $${CPU_COUNT:-`nproc`} -V

test-cider-velox:
	@cd ${CPP_BUILD_DIR}/src/cider-velox/test && \
	ctest -j $${CPU_COUNT:-`nproc`} -V

test-with-arrow-format:
	bash ci/scripts/test_with_arrow_format.sh '${BUILD_TYPE}'

test-debug:
	@$(MAKE) test-cider BUILD_TYPE=Debug
	@$(MAKE) test-cider-velox BUILD_TYPE=Debug

test-release:
	@$(MAKE) test-cider BUILD_TYPE=Release
	@$(MAKE) test-cider-velox BUILD_TYPE=Release

update-copyright:
	sed -i '0,/Copyright.*Intel Corporation/s//Copyright(c) 2022-$(shell date +"%Y") Intel Corporation/' `grep Copyright -rl --exclude-dir="build-*" --exclude-dir="thirdparty" --exclude-dir=".cache" .`

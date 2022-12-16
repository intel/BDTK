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

.PHONY: all clean build debug release \
		test-cider test-cider-velox test-debug test-release

BUILD_TYPE := Release

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
	@sed -i "s/\$${Protobuf_PROTOC_EXECUTABLE} --proto_path \$${PROJECT_SOURCE_DIR}\//protoc --proto_path \$${proto_directory}\//g" ./thirdparty/velox/velox/substrait/CMakeLists.txt
	@sed -i "s/--proto_path \$${Protobuf_INCLUDE_DIR} --cpp_out \$${PROJECT_SOURCE_DIR}/--proto_path \$${Protobuf_INCLUDE_DIR} --cpp_out \$${PROTO_OUTPUT_DIR}/g" ./thirdparty/velox/velox/substrait/CMakeLists.txt
	@sed -i "s/velox\/substrait\/proto\///g" ./thirdparty/velox/velox/substrait/proto/substrait/algebra.proto
	@sed -i "s/velox\/substrait\/proto\///g" ./thirdparty/velox/velox/substrait/proto/substrait/function.proto
	@sed -i "s/velox\/substrait\/proto\///g" ./thirdparty/velox/velox/substrait/proto/substrait/parameterized_types.proto
	@sed -i "s/velox\/substrait\/proto\///g" ./thirdparty/velox/velox/substrait/proto/substrait/plan.proto
	@sed -i "s/velox\/substrait\/proto\///g" ./thirdparty/velox/velox/substrait/proto/substrait/type_expressions.proto
	@sed -i "\$$a\\target_include_directories(velox_dwio_dwrf_proto PUBLIC \$${CMAKE_CURRENT_BINARY_DIR}\/..\/..\/..\/..\/..\/..\/)" ./thirdparty/velox/velox/dwio/dwrf/proto/CMakeLists.txt

	@mkdir -p build-${BUILD_TYPE}
	@cd build-${BUILD_TYPE} && \
	cmake -Wno-dev \
		  -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
		  -DENABLE_FOLLY=OFF \
		  -DENABLE_JIT_DEBUG=OFF \
		  -DENABLE_PERF_JIT_LISTENER=OFF\
		  -DENABLE_INTEL_JIT_LISTENER=OFF \
		  -DPREFER_STATIC_LIBS=OFF \
			-DENABLE_ASN=OFF \
		  -DVELOX_ENABLE_SUBSTRAIT=ON \
		  -DVELOX_ENABLE_PARQUET=ON \
		  $(FORCE_COLOR) \
		  ..

build:
	VERBOSE=1 cmake --build build-${BUILD_TYPE} -j $${CPU_COUNT:-`nproc`} || \
	cmake --build build-${BUILD_TYPE}
	@mkdir -p build-${BUILD_TYPE}/src/cider-velox/function/ && cp -r build-${BUILD_TYPE}/src/cider/function/*.bc build-${BUILD_TYPE}/src/cider-velox/function/

debug:
	@$(MAKE) build-common BUILD_TYPE=Debug
	@$(MAKE) build BUILD_TYPE=Debug

release:
	@$(MAKE) build-common BUILD_TYPE=Release
	@$(MAKE) build BUILD_TYPE=Release

test-cider:
	@cd build-${BUILD_TYPE}/src/cider/tests && \
	ctest -j $${CPU_COUNT:-`nproc`} -V

test-cider-velox:
	@cd build-${BUILD_TYPE}/src/cider-velox/test && \
	ctest -j $${CPU_COUNT:-`nproc`} -V

test-with-arrow-format:
	bash ci/scripts/test_with_arrow_format.sh '${BUILD_TYPE}'

test-debug:
	@$(MAKE) test-cider BUILD_TYPE=Debug
	@$(MAKE) test-cider-velox BUILD_TYPE=Debug

test-release:
	@$(MAKE) test-cider BUILD_TYPE=Release
	@$(MAKE) test-cider-velox BUILD_TYPE=Release

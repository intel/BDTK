/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "util/filesystem/cider_path.h"
#include <linux/limits.h>
#include <unistd.h>
#include <filesystem>
#include "util/Logger.h"

namespace cider {

std::string get_root_abs_path() {
  char abs_exe_path[PATH_MAX] = {0};
  auto path_len = readlink("/proc/self/exe", abs_exe_path, sizeof(abs_exe_path));
  CHECK_GT(path_len, 0);
  CHECK_LT(static_cast<size_t>(path_len), sizeof(abs_exe_path));
  std::filesystem::path abs_exe_dir(std::string(abs_exe_path, path_len));
  abs_exe_dir.remove_filename();
  const auto mapd_root = abs_exe_dir.parent_path().parent_path();
  return mapd_root.string();
}

}  // namespace cider

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Introduction

Big Data Analytic Toolkit is a set of acceleration libraries aimed to optimize big data analytic frameworks.

The following diagram shows the design architecture.
![BDTK-INTRODUCTION](docs/images/BDTK-arch.PNG)

Major components of the project include:

 - [Cider](https://github.com/cider):

   a modularized and general-purposed Just-In-Time (JIT) compiler for data analytic query engine. It employs  [Substrait](https://github.com/substrait-io/substrait) as a protocol allowing to support multiple front-end engines. Currently it provides a LLVM based implementation based on [HeavyDB](https://github.com/heavyai/heavydb).

 - [Velox Plugin](https://github.com/intel-innersource/frameworks.ai.modular-sql.velox-plugin/blob/main/cider-velox/README.MD):

   a Velox-plugin is a bridge to enable Big Data Analytic Toolkit onto [Velox](https://github.com/facebookincubator/velox/commits/main). It introduces hybrid execution mode for both compilation and vectorization (existed in Velox). It works as a plugin to Velox seamlessly without changing Velox code.

 - [Intel Codec Library](https://github.com/Intel-bigdata/IntelCodecLibrary): Intel Codec Library for BigData provides compression and decompression library for Apache Hadoop/Spark to make use of the acceleration hardware for compression/decompression.

# Cider & Velox Plugin

## Major API Example

## How to build

## How to Enable in Presto

# Code Of Conduct

# Online Documentation

You can find the all the Big Data Analytic Toolkit documents on the [project web page](https://silver-meme-cac8f6b3.pages.github.io/).

# License

Big Data Analytic Toolkit is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)


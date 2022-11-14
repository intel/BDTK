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
# Intel Big Data Analytic Toolkit
Intel Big Data Analytic Toolkit (abbrev. BDTK) is a set of acceleration libraries aimed to optimize big data analytic frameworks. 

By using this library, frontend SQL engines like Prestodb/Spark query performance will be significant improved. 

## Problem Statement

For big data analytic framework users, it becomes more and more significant needs for better performance. And most of existing big data analytic frameworks are built via Java and it's designed mostly for CPU only computation. To unblock performance further to hardware bare metal, native implementation and leveraging state-of-art hardwares are employed in this toolkit. 

Furthermore, assembling & building becomes a new trend for data analytic solution providers. More and more SQL based solutions were built based on some primitive building blocks over the last five years. Having some performt OOB building blocks (as libraries) can significantly reduce time-to-value for building everything from scratch. With such general-purpose toolkit, it can significantly reduce time-to-value for analytic solution developers.

## Targeted Use Cases

BDTK focuses on following areas:
-	End-users of big data analytic frameworks who're looking for performance acceleration
-	Data engineers who want some Intel architecture based optimizations
-	Database developers who're seeking for reusable building blocks
-	Data Scientist who looks for heterogenous execution

Users can reuse implemented operators/functions to build a full-featured SQL engine. Currently this library offers an highly optimized compiler to JITed function for execution.
 
Building blocks utilizing compression codec (based on IAA, QAT) can be used directly to Hadoop/Spark for compression acceleration.

# Introduction
The following diagram shows the design architecture. Currently, it offers a few building blocks including a lightweight LLVM based SQL compiler on top of Arrow data format, ICL - a compression codec leveraging Intel IAA accelerator,  QATCodec - compression codec wrapper based on Intel QAT accelerator. 

 - [Cider](https://github.com/intel/BDTK/tree/main/cider):

   a modularized and general-purposed Just-In-Time (JIT) compiler for data analytic query engine. It employs  [Substrait](https://github.com/substrait-io/substrait) as a protocol allowing to support multiple front-end engines. Currently it provides a LLVM based implementation based on [HeavyDB](https://github.com/heavyai/heavydb).

 - [Velox Plugin](https://github.com/intel/BDTK/tree/main/cider-velox):

   a Velox-plugin is a bridge to enable Big Data Analytic Toolkit onto [Velox](https://github.com/facebookincubator/velox). It introduces hybrid execution mode for both compilation and vectorization (existed in Velox). It works as a plugin to Velox seamlessly without changing Velox code.

 - [Intel Codec Library](https://github.com/Intel-bigdata/IntelCodecLibrary):

   Intel Codec Library for BigData provides compression and decompression library for Apache Hadoop/Spark to make use of the acceleration hardware for compression/decompression.

![BDTK-INTRODUCTION](docs/images/BDTK-arch.PNG)

# Supported Features
Current supported features are available on [Project Page](https://intel.github.io/BDTK/user/function-support.html). Newly supported feature in release 0.9 is available at [release page](https://github.com/intel/BDTK/releases/tag/v0.9.0). 

# Getting Started

## Get the BDTK Source
```
git clone --recursive https://github.com/intel/BDTK.git
cd BDTK
# if you are updating an existing checkout
git submodule sync --recursive
git submodule update --init --recursive
```

## Setting up BDTK develop envirenmont on Linux Docker

We provide Dockerfile to help developers setup and install BDTK dependencies.

1. Build an image from a Dockerfile
```shell
$ cd ${path_to_source_of_bdtk}/ci/docker
$ docker build -t ${image_name} .
```
2. Start a docker container for development
```shell
$ docker run -d --name ${container_name} --privileged=true -v ${path_to_source_of_bdtk}:/workspace/bdtk ${image_name} /usr/sbin/init
```
## How To Build
Once you have setup the Docker build envirenment for BDTK and get the source, you can enter the BDTK container and build like:

Run `make` in the root directory to compile the sources. For development, use
`make debug` to build a non-optimized debug version, or `make release` to build
an optimized version.  Use `make test-debug` or `make test-release` to run tests.

## How to Enable in Presto
To use it with Prestodb, Intel version [Prestodb](https://github.com/intel-bigdata/presto/) is required together with Intel version [Velox](https://github.com/intel-bigdata/velox). Detailed steps are available at [installation guide](https://intel.github.io/BDTK/user/quick-start).

# Roadmap
In the next coming release, following working items were prioritized.
-	Better test coverage for entire library
-	Better robustness and enable more implemented features in Prestodb as pilot SQL engine, by improving offloading framework
-	Better extensibility at multi-levels (incl. relational algebra operator, expression function, data format), by adopting state-of-art compiler design (multi-levels) 
-	Complete Arrow format migration
-	Next-gen codegen framework
-	Support large volume data processing
-	Advanced features development

# Code Of Conduct
Big Data Analytic Toolkit's Code of Conduct [can be found here.](CODE_OF_CONDUCT.md)

# Online Documentation

You can find the all the Big Data Analytic Toolkit documents on the [project web page](https://intel.github.io/BDTK/).

# License

Big Data Analytic Toolkit is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)


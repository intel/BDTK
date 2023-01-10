.. Copyright (c) 2022 Intel Corporation.
..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _icl_user_guide_reference_link:

==============================
Intel Codec Library User Guide
==============================

.. _icl_description_reference_link:

Module description
******************

The Intel Codec Library (ICL) provides compression and decompression library for Apache Hadoop/Spark/Parquet and so on to make use of the hardware accelerator, and/or software for compression/decompression. It not only supports the use of Intel hardware accelerator such as QAT and IAA to accelerate the deflate-compatible data compression algorithm but also supports the use of Intel optimized software solutions such as Intel ISA-L(Intel Intelligent Storage Acceleration Library) and IPP(Intel Integrated Performance Primitives Library) to accelerate the data compression.

Currently the Intel Codec Library supports the following backends:

- ``IGZIP`` - use ISA-L software to accelerate the compression  
- ``QPL`` - use IAA hardware accelerator to accelerate the compression
- ``QAT`` - use QAT hardware accelerator to accelerate the compression
- ``IPP`` - use IPP software to accelerate the compression. **Note:** ``A license is needed to use IPP``

.. _icl_sdk_api_reference_link:

SDK API
*******

The class diagram of Intel Codec Library is shown below:

.. image:: ../../images/ICL-class.png

.. doxygenclass:: icl::codec::IclCompressionCodec
   :members:

Building the Library
********************

Prerequisites
=============

Before building Intel Codec Library, install and set up the following tools:

- nasm 2.14.0 or higher (e.g., can be obtained from https://www.nasm.us)

- A C++17-enabled compiler. GCC 8 and higher should be sufficient

- CMake version 3.16 or higher

- ``libaccel-config`` library version 3.2 or higher is required for building Intel Codec Library with QPL support. Refer to `accel-config releases <https://github.com/intel/idxd-config/>`__ for how to install.

Build
=====

To build Intel Codec Library, complete the following step:

1. Get the Source using the following command:

   .. code-block:: shell

      git clone --recursive https://github.com/intel/BDTK.git <BDTK>

2. Build the library and tests by executing the following commands in ``<BDTK>``

   .. code-block:: shell

      mkdir build
      cd build
      cmake -DCMAKE_BUILD_TYPE=Release -DBDTK_ENABLE_ICL=ON -DBDTK_ENABLE_CIDER=OFF -DCMAKE_INSTALL_PREFIX=<install_dir> ..
      cmake --build . --target install

   .. note::

      You need to see more options to customize your build. See :ref:`icl_build_options_reference_link` for details.

.. _icl_build_options_reference_link:

Build Options
=============

By default, the C++ build system creates a fairly minimal build. Intel Codec Library supports the following build options which you can opt into building by passing
boolean flags to ``cmake``.

-  ``-DICL_WITH_QPL=[ON|OFF]`` - Build ICL with QPL codec (``OFF`` by default).
-  ``-DICL_WITH_QAT=[ON|OFF]`` - Build ICL with QAT codec (``OFF`` by default).
-  ``-DICL_WITH_IGZIP=[ON|OFF]`` - Build ICL with IGZIP codec (``ON`` by default).
-  ``-DICL_BUILD_TESTS=[ON|OFF]`` - Build the ICL googletest unit tests (``OFF`` by default).
-  ``-DICL_BUILD_BENCHMARKS=[ON|OFF]`` - Build the ICL micro benchmarks (``OFF`` by default).

Modular Build Target
====================

Since there are several suport codes of the ICL, we have provided
modular CMake target for building each supported codecs, group of unit tests
and benchmarks:

* ``make icl`` for ICL libraries which enable only *igzip* codec support.
* ``make icl-qpl`` for ICL libraries which enable only *qpl* codec support.
* ``make icl-qat`` for ICL libraries which enable only *qat* codec support.
* ``make icl-all`` for ICL libraries which enable all supported codes.

.. _icl_how_to_use_the_library_reference_link:

How to use Intel Codec Library
******************************

We provided several ways to use Intel Codec Library:

* ICL C++ API
* ICL JAVA API
* ICL `Arrow Compression Codec API <https://github.com/apache/arrow/blob/master/cpp/src/arrow/util/compression.h>`__  

ICL C++ API
===========

Intel Codec Library provides C++ API that the end users can directly call the C++ API to integrate into their own applications.

Let's walk through the below example that compresses and decompresses data with `igzip` codec to learn the basic workflow of Intel Codec Library C++ API.

.. code-block:: cpp

   #include "icl/icl.h"

   auto codec = IclCompressionCodec::MakeIclCompressionCodec("igzip", 1);

   int max_compressed_len =
      static_cast<int>(codec->MaxCompressedLen(data.size(), data.data()));

   std::vector<uint8_t> compressed(max_compressed_len);
   std::vector<uint8_t> decompressed(data.size());

   int64_t actual_size =
      codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data());
   compressed.resize(actual_size);

   int64_t actual_decompressed_size = codec->Decompress(
      compressed.size(), compressed.data(), decompressed.size(), decompressed.data());

To work with Intel Codec Library C++ API, the application will need to:

1. The application only needs to include one header file ``icl/icl.h``, which specifies the prototypes of all the functions. 
2. Call `IclCompressionCodec::MakeIclCompressionCodec()` to create the instance of IclCompressionCodec, you can pass the required underlying codec and the compression level as parameters to this function. 
3. Call `MaxCompressedLen()` to query the required compressed buffer size
4. Allocate compressed buffer according to the returned value of step 3. 
5. Call `Compress()` to perform a compression operation for the input data buffer and return the actual compressed size.
6. Or call `Decompress()` to perform a decompression operation. 
7. Free resources.

ICL JAVA API
============

To Be Added.


.. _icl_arrow_reference_link:

ICL Arrow Compression Codec API
========================================

We also provides an Arrow patch that enable the Arrow Compression Codec to leverage the Intel Codec Library to accelerate the Arrow GzipCodec. Softwares(e.g., the native parquet reader, the Arrow IPC, the Arrow Flight etc.) that use Arrow Compression Codec can get performance boost without any code modify and simply replacing the Arrow library.   

To use Arrow Compression Codec with Intel Codec Library, users need rebuild Arrow following the below guide:

1. Build Intel Codec Library using the following command:

   .. code-block:: shell

     make icl 

   .. note::

     * Please see :ref:`icl_building_the_library_reference_link` for how to customize your build and enable required codec. 
     * Please make sure to turn off the option ``-DICL_BUILD_TESTS=OFF`` and ``-DICL_BUILD_BENCHMARKS=OFF`` when build 
2. Download the Arrow patch `here <https://github.com/intel/BDTK/src/compression/build-support/0001-Add-ICL-support.patch>`__.
3. Get the Arrow Source using the following command:

   .. code-block:: shell

      git clone -b apache-arrow-10.0.0 https://github.com/apache/arrow.git

   .. note::

     Currently, we only provide the patch for Arrow version *10.0.0*, other versions please make corresponding modifications based on this patch.
4. Apply the patch using the following command: 

   .. code-block:: shell

      cd arrow
      git am 0001-Add-ICL-support.patch
5. Build Arrow with ``-DARROW_WITH_ICL=ON`` option

   .. note::

      * Please ref `here <https://arrow.apache.org/docs/developers/cpp/building.html>`__ for how to build arrow.
6. Set the environment ``GZIP_BACKEND`` to enable Intel Codec Library, for example, to enable ``igzip`` codec, you can set ``GZIP_BACKEND`` with the following command:

   .. code-block:: shell

     export GZIP_BACKEND="igzip" 

   .. note::

     * The supported ``GZIP_BACKEND`` can be set  to one of ``igzip``, ``qpl``, or ``qat``
     * The Arrow Codec must be set to ``Compression::GZIP`` 
7. Use Arrow Codec API as below example:

 .. code-block:: cpp

   #include "arrow/util/compression.h"

   ARROW_ASSIGN_OR_RAISE(auto codec, Codec::Create(Compression::GZIP, 1));

   std::vector<uint8_t> compressed(max_compressed_len);
   std::vector<uint8_t> decompressed(data.size());
   int max_compressed_len =
      static_cast<int>(codec->MaxCompressedLen(data.size(), data.data()));

   ARROW_ASSIGN_OR_RAISE(
      auto compressed_size,
      codec->Compress(data.size(), data.data(), max_compressed_len, compressed_data.data()));

   ARROW_ASSIGN_OR_RAISE(
      auto decompressed_size,
      codec->Decompress(compressed.size(), compressed.data(), decompressed.size(), decompressed.data()));

How to run benchmark
********************

Intel Codec Library provided two types of benchmarks: 

* Benchmark for normal file  
* Benchmark for Parquet file 

Benchmark for normal file
=========================

This benchmark will read ``Calgary corpus`` data which commonly used for comparing data compression algorithms as input and use different compressors supported by Arrow to compress and decompress that data to benchmark different codec's performance.

To run the benchmark following the below guide:

1. Build Intel Codec Library with benchmark option on:  

   .. code-block:: shell

      mkdir build
      cd build
      cmake -DCMAKE_BUILD_TYPE=Release -DBDTK_ENABLE_ICL=ON -DICL_BUILD_BENCHMARKS=ON -DBDTK_ENABLE_CIDER=OFF ..
      cmake --build .

   .. note::

     * To build benchmark, you need build Arrow first, please see :ref:`icl_arrow_reference_link` for how to build Arrow. 
     * Please see :ref:`icl_building_the_library_reference_link` for how to customize your build and enable required codec. 
     * Please make sure to turn on the option ``-DICL_BUILD_BENCHMARKS=ON`` when build 
     * Please make sure build ICL with the correct Arrow package which build with ICL patch, you can set the environment ``ARROW_HOME`` to the folder where installed the Arrow with ICL patch.
2. Prepare the ``Calgary corpus`` data for benchmark
   You can download the data from `here <http://www.data-compression.info/files/corpora/largecalgarycorpus.zip>`__, then unzip it and tar these files to a tar file ``calgary.tar``
3. Copy the calgary.tar to folder where you run the CompressionBenchmark which generated in step 1.

   .. note::

     * To enable ICL, you need set the environment ``GZIP_BACKEND`` when run the benchmark. 


Benchmark for parquet file
==========================

To be Added

Limitation
**********

In order to take full advantage of hardware acceleration, Intel Codec Library only supports block-based interface for compression, aka “one shot”, so the data compressed by ICL can’t be decompressed by stream-based software like Gzip. 

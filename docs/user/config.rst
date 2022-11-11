=====================
How to use
=====================

BDTK mainly acts as a plugin on velox right now, the major way for it to integrate with Presto is to compile with Velox among the Prestissimo project.
In this context and in the following guide, the term **presto_cpp** or **presto native worker** stands for Presto + Velox integrated with BDTK.

Environment Preparation
-----------------------------------

| **Prerequisite:**
| Install docker and set proxy according to the guidance in:
| https://docs.docker.com/config/daemon/systemd/
| https://docs.docker.com/network/proxy/

1. Get the Source

::

   # Clone the Presto source
   $ git clone https://github.com/Intel-bigdata/presto.git
   # Checkout to BDTK branch
   $ git checkout -b BDTK origin/BDTK

2. Setting up BDTK develop envirenmont on Linux Docker

We provide Dockerfile to help developers setup and install BDTK dependencies.

::

   # Build an image from a Dockerfile
   $ cd ${path_to_source_of_bdtk}/ci/docker
   $ docker build -t ${image_name} .

   # Start a docker container for development
   docker run -d --name ${container_name} --privileged=true -v ${path_to_bdtk}:/workspace/bdtk -v ${path_to_presto}:/workspace/presto ${image_name} /usr/sbin/init
   # Tips: you can run with more CPU cores to accelerate building time
   # docker run -d ... ${image_name} --cpus="30" /usr/sbin/init

   docker exec -it ${container_name} /bin/bash

*Note: files used for building image are from bdtk and presto,
details are as follows:*


Run with Prestodb
-----------------------------------
Integrate BDTK with Presto
^^^^^^^^^^^^^^^^^^^^^^^^^^^

*Note: The following steps should be done in the docker container*

::

   $ cd ${path-to-presto}/presto-native-execution
   # Integrate BDTK with Presto
   $ export WORKER_DIR=${path-to-presto}/presto-native-execution
   $ bash ${WORKER_DIR}/BDTK/ci/scripts/integrate-presto-bdtk.sh release

Now the you can check your executable presto server file in ${WORKER_DIR}/_build/release/presto_cpp/main/presto_server


Run a end-to-end Hello World demo on local file system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
*Note: The following steps should be done in the docker container*

| **Prerequisite:**
| Java 8
| Maven 3.5.x and later version

1. Compile Prestodb

::

   $ cd ${path-to-presto}
   $ mvn clean install -DskipTests


Advanced Settings
------------------

There have four pattern configurations now in our project:

* LeftDeepJoinPattern
* CompoundPattern
* FilterPattern
* PartialAggPattern

We enable ``CompoundPattern`` and ``FilterPattern`` by default.

If you want to change the default value of these patterns, there have two ways :

1. Write a file firstly, such as pattern.flags.
    ::

        --PartialAggPattern
        --CompoundPattern=false

    And then you could use it like this:
    ::
        ./presto_server --flagfile=/path/to/pattern.flags

2. Just change them on command line.

        ./presto_server --PartialAggPattern --CompoundPattern=false

*Note: You also can find the definition of them from file CiderPlanTransformerOptions.cpp.*


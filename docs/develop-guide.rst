Cider Developer Guide
======================

1 How to build
--------------

1.1 Build BDTK
~~~~~~~~~~~~~~~~~~~~~

| **Prerequisite:**
| Install docker and set proxy according to the guidance in:
| https://docs.docker.com/config/daemon/systemd/
| https://docs.docker.com/network/proxy/

1. Get the BDTK Source

::

   $ git clone --recursive https://github.com/intel/BDTK.git
   $ cd BDTK
   # if you are updating an existing checkout
   $ git submodule sync --recursive
   $ git submodule update --init --recursive

2. Setting up BDTK develop envirenmont on Linux Docker

   We provide Dockerfile to help developers setup and install BDTK dependencies.

::

   # Build an image from a Dockerfile
   $ cd ${path_to_source_of_bdtk}/ci/docker
   $ docker build -t ${image_name} .

   # Start a docker container for development
   docker run -d --name ${container_name} --privileged=true -v ${path_to_bdtk}:/workspace/bdtk ${image_name} /usr/sbin/init
   # Tips: you can run with more CPU cores to accelerate building time
   # docker run -d ... ${image_name} --cpus="30" /usr/sbin/init

   docker exec -it ${container_name} /bin/bash

*Note: files used for building image are from cider and presto,
details are as follows:*

3. Build in container

   Once you have setup the Docker build envirenment for BDTK and get the source, you can enter the BDTK container and build like:

   Run ``make`` in the root directory to compile the sources. For development, use ``make debug`` to build a non-optimized debug version, or ``make release`` to build an optimized version. Use ``make test-debug`` or ``make test-release`` to run tests.

4. Setup ssh for debugging

::

   # Start ssh server in the docker
   systemctl start ssh
   ssh-keygen -t rsa # help create .ssh folder 
   systemctl enable ssh
   # Configure bypass ssh to docker    
   # Configure remote debug referring https://www.jianshu.com/p/0f2fb935a9a1   

2 How to run unit tests
-----------------------

2.1 How to run all unit tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For BDTK:

::

   # WORKDIR: /workspace/bdtk
   # test both cider and cider-velox
   make test-debug/release

   # only test cider
   make test-cider BUILD_TYPE=Debug/Release /test-cider-velox 

   # only test cider-velox 
   make test-cider-velox BUILD_TYPE=Debug/Release 

   # Note: If you want to test both Debug and Release, just use '&&' to connect two commands.


2.2 How to run a single unit test

::
   
   # For Cider
   # Test Path: /workspace/bdtk/build-$BUILD_TYPE/cider/tests
   ./XXXtest 

   # For CiderVelox
   # Test Path: /workspace/bdtk/build-$BUILD_TYPE/cider-velox/tests

   # Please check is there a function directory(auto generate when make test-$BUILD_TYPE) under cider-velox.
   # If not, please use follow commands to get one(under /workspace/bdtk/): 
   #   mkdir /workspace/bdtk/build-${BUILD_TYPE}/cider-velox/function
   #   cp /workspace/bdtk/build-${BUILD_TYPE}/cider/function/*.bc /workspace/bdtk/build-{BUILD_TYPE}/cider-velox/function
   #   cd /workspace/bdtk/build-${BUILD_TYPE}cider-velox/test
   ./XXXtest 

Then configure GDB with the built test binaries, like following:

::

   {
       // Use IntelliSense to learn about possible attributes.
       // Hover to view descriptions of existing attributes.
       // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
       "version": "0.2.0",
       "configurations": [
       {
         "name": "${name_of_this_configuration}",
         "type": "cppdbg",
         "request": "launch",
         "program": "${path_to_the_test_you_want_to_debug}",
         "args": [],
         "stopAtEntry": false,
         "cwd": "${workspaceFolder}",
         "environment": [],
         "externalConsole": false,
         "MIMode": "gdb",
         "setupCommands": [
             {
                 "description": "Enable pretty-printing for gdb",
                 "text": "-enable-pretty-printing",
                 "ignoreFailures": true
             }
         ]
       }
       ]
   }

3 How to install
----------------

TODO:

4 How to debug
--------------

4.1 Debug cider in vscode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enable debug option when make: ``cmake  -DCMAKE_BUILD_TYPE=Debug ..``
Configure ``.vscode/launch.json``

::

   {
       // Use IntelliSense to learn about possible attributes.
       // Hover to view descriptions of existing attributes.
       // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
       "version": "0.2.0",
       "configurations": [
       {
         "name": "${name_of_this_configuration}",
         "type": "cppdbg",
         "request": "launch",
         "program": "${path_to_the_test_you_want_to_debug}",
         "args": [],
         "stopAtEntry": false,
         "cwd": "${workspaceFolder}",
         "environment": [],
         "externalConsole": false,
         "MIMode": "gdb",
         "setupCommands": [
             {
                 "description": "Enable pretty-printing for gdb",
                 "text": "-enable-pretty-printing",
                 "ignoreFailures": true
             }
         ]
       }
       ]
   }


Then “Run and debug”

4.2 Debug in CLion
~~~~~~~~~~~~~~~~~~

(1) Configure Toolchain

(2) Configure CMake, set build type as ``Debug`` and build directory to
    be “build” *Note: you should have already built cider binary under
    “build” dir, otherwise, you need configure Clion bundled cmake task
    to launch build task.* 

(3) Choose ExecuteTest for example and start debug, set up breakpoints
    and step in/over

4.3 Remote debug in Docker image with Clion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO:

5 How to get LLVM IR
--------------------


6 How to run simple examples with Prestodb in DEV environment
-------------------------------------------------------------

6.1 Configure Hive MetaStore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Follow the steps from
https://prestodb.io/docs/current/installation/deployment.html#configuring-presto
to install Hive metastore (requiring HDFS pre-installed)

Download and extract the binary tarball of Hive. For example, download
and untar ``apache-hive-<VERSION>-bin.tar.gz``

You only need to launch Hive Metastore to serve Presto catalog
information such as table schema and partition location. If it is the
first time to launch the Hive Metastore, prepare corresponding
configuration files and environment, also initialize a new Metastore:

::

   export HIVE_HOME=`pwd`
   cp conf/hive-default.xml.template conf/hive-site.xml
   mkdir -p hcatalog/var/log/
   # only required for the first time
   bin/schematool -dbType derby -initSchema

Start a Hive Metastore which will run in the background and listen on
port 9083 (by default).

::

   hcatalog/sbin/hcat_server.sh start
   # Output: 
   # Started metastore server init, testing if initialized correctly...
   # Metastore initialized successfully on port[9083].

6.2 Prepare Cider as library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

6.2.2 Resolve dependency
^^^^^^^^^^^^^^^^^^^^^^^^

Copy ``$CIDER_BUILD_DIR/function`` to ``$JAVA_HOME/``
may need ``function/*.bc`` files

6.3 Configure Prestodb server and run some example queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Follow steps from
https://github.com/intel-bigdata/presto/tree/cider#running-presto-in-your-ide

6.3.1 Running with IDE
^^^^^^^^^^^^^^^^^^^^^^

After building Presto for the first time, you can load the project into
your IDE and run the server. We recommend using `IntelliJ
IDEA <http://www.jetbrains.com/idea/>`__. Because Presto is a standard
Maven project, you can import it into your IDE using the root
``pom.xml`` file. In IntelliJ, choose Open Project from the Quick Start
box or choose Open from the File menu and select the root ``pom.xml``
file.

After opening the project in IntelliJ, double check that the Java SDK is
properly configured for the project: \* Open the File menu and select
Project Structure \* In the SDKs section, ensure that a 1.8 JDK is
selected (create one if none exist) \* In the Project section, ensure
the Project language level is set to 8.0 as Presto makes use of several
Java 8 language features

Presto comes with sample configuration that should work out-of-the-box
for development. Use the following options to create a run
configuration: \* Main Class: com.facebook.presto.server.PrestoServer \*
VM Options:
``-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties``
\* Working directory: ``$MODULE_DIR$`` \* Use classpath of module:
presto-main

The working directory should be the ``presto-main`` subdirectory. In
IntelliJ, using ``$MODULE_DIR$`` accomplishes this automatically.
Additionally, the Hive plugin must be configured with location of your
Hive metastore Thrift service. Add the following to the list of VM
options, replacing ``localhost:9083`` with the correct host and port (or
use the below value if you do not have a Hive metastore):
``-Dhive.metastore.uri=thrift://localhost:9083``

6.3.2 How to improve Prestodb initialization speed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Speed up presto init Presto server will load a lot plugin and it will
resolve dependency from maven central repo and this is really slow. A
solution is to modify this class and bypass resolve step.

::

   git clone -b offline https://github.com/jikunshang/resolver.git
   cd resolver
   mvn clean install -DskipTests=true
   # change resolver version in pom file
   # presto/pom.xml L931    <version>1.4</version> ->   <version>1.7-SNAPSHOT</version>
   And you can remove unnecessary catlog/connector by remove source/presto-main/etc/catalog/*.properties and source/presto-main/etc/catalog/config.properties  plugin.bundles=

6.3.3 Running filter/project queries with CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start the CLI to connect to the server and run SQL queries:
``presto-cli/target/presto-cli-*-executable.jar`` Run a query to see the
nodes in the cluster:

::

   SELECT * FROM system.runtime.nodes;

   presto> create table hive.default.test(a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO test VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   set session hive.pushdown_filter_enabled=true;
   presto> select * from hive.default.test where c > 12;

6.3.4 Running join queries with CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start the CLI to connect to the server and run SQL queries:

::

   presto-cli/target/presto-cli-*-executable.jar
   presto> create table hive.default.test_orc1(a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO hive.default.test_orc1 VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   presto> SET SESSION join_distribution_type = 'PARTITIONED';
   presto> create table hive.default.test_orc2 (a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO hive.default.test_orc2 VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   presto> select * from hive.default.test_orc1 l, hive.default.test_orc2 r where l.a = r.a;

7 How to run simple examples with Prestodb in distributed environment
---------------------------------------------------------------------

(1) Create a folder to install cider files, for example ``cider``

(2) | Copy the lib folder under the cider docker build environment to
      every node, for example, copy
      ``/usr/local/lib/`` folder to
      ``/path/to/cider`` on every Prestodb node

::

   cp -a /usr/local/lib/ /path/to/cider

(4) Copy the ``ExtensionFunctions.ast``, and
    ``RuntimeFunctions.bc`` from the function folder under the
    cider build folder to function folder

::

   cp /path/to/cider/build/function/ExtensionFunctions.ast   /path/to/cider/function
   cp /path/to/cider/build/function/RuntimeFunctions.bc   /path/to/cider/function

(7) Set the LD_LIBRARY_PATH environment variable include the lib folder.

::

   export LD_LIBRARY_PATH=/path/to/cider/lib:$LD_LIBRARY_PATH

(8) You may also need include the libjvm.so in your LD_LIBRARY_PATH if
    it is not

::

   export LD_LIBRARY_PATH= $JAVA_HOME/jre/lib/amd64/server/:$LD_LIBRARY_PATH


8 How to contribute document
-----------------------------

8.1 Introduction
~~~~~~~~~~~~~~~~~

Cider documentation uses sphinx to produce html structure.
Github pages refer to "docs" directory on "gh-pages" branch.

8.2 Build and commit
~~~~~~~~~~~~~~~~~~~~~

We maintain gh-pages with github actions, which is implemented in .github/workflows/update-gh-pages.yml.

We can simply edit rst files under "docs" directory, when the change merge to "main" branch,
github action will automatically build gh-pages.

If you want to add a new rst file, remember add its title to "index.rst". 

8.3 External links
~~~~~~~~~~~~~~~~~~~

Last, share some tools and documents, hope it can help:

1.  Sphinx quick start: `sphinx-doc <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`_

2.  How to write rst(reStructuredText) files: `rst-tutorial <https://www.devdungeon.com/content/restructuredtext-rst-tutorial-0>`_

3.  Transfer markdown to rst: `md-to-rst <https://cloudconvert.com/md-to-rst>`_


9 Troubleshooting
-----------------

9.1 Cider Velox docker build failed issue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

9.1.1 The rapidjson build failed issue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

error like blow. Fix it by rapidjson/rapidjson/document.h:2244:22: note:
candidate: ’template rapidjson::GenericDocument<Encoding, Allocator,
StackAllocator>& rapidjson::GenericDocument<Encoding, Allocator,
StackAllocator>::Parse(const Ch\ *) [with unsigned int parseFlags =
parseFlags; Encoding = rapidjson::UTF8<>; Allocator =
rapidjson::MemoryPoolAllocator<>; StackAllocator =
rapidjson::CrtAllocator]’ 2244 \| GenericDocument& Parse(const Ch* str)
{ \| ^~~~~

9.1.2 Maven can’t parse proxy correctly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| If it raises error related to proxy by Maven, please ensure your
  settings.xml file (usually ${user.home}/.m2/settings.xml) is secured
  with permissions appropriate for your operating system.
| Reference: https://maven.apache.org/guides/mini/guide-proxies.html

9.1.3 The Velox build failed issue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

(1) If it raises error on the code in velox/velox/core/Context.h,
	please modified the corresponding code like this:

::

   enum class ContextScope { GLOBAL, SESSION, QUERY, SCOPESTACK };
   saying: expected identifier before ‘,’ token, please make a modification:
   enum class UseCase {
   DEV = 1,
   TEST = 2,
   PROD = 3,
   };
       
   #ifdef GLOBAL
   #undef GLOBAL
   #endif
   enum class ContextScope { GLOBAL, SESSION, QUERY, SCOPESTACK };

9.2 Prestodb Internals
~~~~~~~~~~~~~~~~~~~~~~

10 Code Style Check
------------------------
Code style check will be triggered automatically after you submit a PR. So please ensure your PR does not break any of these workflows. It runs ``make format-check``, ``make header-check`` as part of our continuous integration. 
Pull requests should pass ``format-check`` and ``header-check`` without errors before being accepted.

More details can be found at `ci/scripts/run_cpplint.py <https://github.com/intel-innersource/frameworks.ai.modular-sql.velox-plugin/blob/40591b915bfee8068749218725f9c95a4704bacd/ci/scripts/run_cpplint.py>`_

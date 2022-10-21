Debugging/Testing
======================

How to build
--------------

Get the BDTK Source
~~~~~~~~~~~~~~~~~~~~~

::

   $ git clone --recursive https://github.com/intel/BDTK.git
   $ cd BDTK
   # if you are updating an existing checkout
   $ git submodule sync --recursive
   $ git submodule update --init --recursive

Setting up BDTK develop envirenmont on Linux Docker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We provide Dockerfile to help developers setup and install BDTK dependencies.

::

   # Build an image from a Dockerfile
   $ cd ${path_to_source_of_bdtk}/ci/docker
   $ docker build -t ${image_name} .

   # Start a docker container for development
   docker run -d --name ${container_name} --privileged=true -v ${path_to_bdtk}:/workspace/bdtk ${image_name} /usr/sbin/init

   docker exec -it ${container_name} /bin/bash

Build in container
~~~~~~~~~~~~~~~~~~~

Once you have setup the Docker build envirenment for BDTK and get the source, you can enter the BDTK container and build like:
Run ``make`` in the root directory to compile the sources. For development, use ``make debug`` to build a non-optimized debug version, or ``make release`` to build an optimized version. Use ``make test-debug`` or ``make test-release`` to run tests.

How to test
-----------------------

How to run all unit tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   # WORKDIR: /workspace/bdtk
   # test both cider and cider-velox
   make test-debug/release

   # only test cider
   make test-cider BUILD_TYPE=Debug/Release /test-cider-velox 

   # only test cider-velox 
   make test-cider-velox BUILD_TYPE=Debug/Release 

   # Note: If you want to test both Debug and Release, just use '&&' to connect two commands.

How to run a single unit test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

How to debug a single unit test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
configure GDB with the built test binaries, like following:

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

How to run simple examples with Prestodb in DEV environment
-------------------------------------------------------------

Configure Hive MetaStore
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

Prepare Cider as library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Resolve dependency, Copy ``$CIDER_BUILD_DIR/function`` to ``$JAVA_HOME/`` may need ``function/*.bc`` files

Configure Prestodb server and run some example queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Follow steps from
https://github.com/intel-bigdata/presto/tree/cider#running-presto-in-your-ide

**Running with IDE**

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

**How to improve Prestodb initialization speed**

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

**Running filter/project queries with CLI**

Start the CLI to connect to the server and run SQL queries:
``presto-cli/target/presto-cli-*-executable.jar`` Run a query to see the
nodes in the cluster:

::

   SELECT * FROM system.runtime.nodes;

   presto> create table hive.default.test(a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO test VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   set session hive.pushdown_filter_enabled=true;
   presto> select * from hive.default.test where c > 12;

**Running join queries with CLI**

Start the CLI to connect to the server and run SQL queries:

::

   presto-cli/target/presto-cli-*-executable.jar
   presto> create table hive.default.test_orc1(a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO hive.default.test_orc1 VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   presto> SET SESSION join_distribution_type = 'PARTITIONED';
   presto> create table hive.default.test_orc2 (a int, b double, c int) WITH (format = 'ORC');   
   presto> INSERT INTO hive.default.test_orc2 VALUES (1, 2, 12), (2, 3, 13), (3, 4, 14), (4, 5, 15), (5, 6, 16);
   presto> select * from hive.default.test_orc1 l, hive.default.test_orc2 r where l.a = r.a;

How to run simple examples with Prestodb in distributed environment
---------------------------------------------------------------------

Build presto native execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Copy ci/build-presto-package.sh to an empty folder and run it. 
Generate Prestodb.tar.gz archive

Unzip the Prestodb package and enter the unzip package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   tar -zxvf Prestodb.tar.gz
   cd Prestodb

Set the LD_LIBRARY_PATH environment variable include the lib folder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH

Run presto_server with parameter point to etc folder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::
   
   ./bin/presto_server -etc_dir=./etc

Troubleshooting
-----------------

Maven can’t parse proxy correctly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

| If it raises error related to proxy by Maven, please ensure your
  settings.xml file (usually ${user.home}/.m2/settings.xml) is secured
  with permissions appropriate for your operating system.
| Reference: https://maven.apache.org/guides/mini/guide-proxies.html

The Velox build failed issue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If it raises error on the code in velox/velox/core/Context.h, please modified the corresponding code like this:

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

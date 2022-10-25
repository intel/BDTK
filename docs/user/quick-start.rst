=====================
BDTK User Guide
=====================

BDTK mainly act as a plugin on velox right now, the major way for it to integrate with Presto is to compile with Velox
among the Prestissimo project. 
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
   docker run -d --name ${container_name} --privileged=true -v ${path_to_bdtk}:/workspace/bdtk ${image_name} -v ${path_to_presto}:/workspace/presto ${image_name} /usr/sbin/init
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
   $ bash ${WORKER_DIR}/velox-plugin/ci/scripts/integrate-presto-bdtk.sh release

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

2. Set up in IntelliJ 
Download and install IntellliJ You can also use any other IDE however the instructions in this document will only concern IntelliJ.

a. Open IntelliJ and use ‘Open Existing’ to open the presto project: Click File > New > Module From Existing Sources .. > , Then go to presto_cpp/java/presto-native-tests/pom.xml. 
b. Now lets create the configuration for HiveExternalWorkerQueryRunner. We will need three env variables for this purpose, so copy the following below and replace the text in bold with your specific text.

   i. Env Variables: PRESTO_SERVER=<YOUR_PATH_TO_PRESTO_SERVER>;DATA_DIR=/Users/<YOUR_USER_NAME>/Desktop;WORKER_COUNT=0

   ii. VM Options: -ea -Xmx2G -XX:+ExitOnOutOfMemoryError -Duser.timezone=America/Bahia_Banderas -Dhive.security=legacy

   iii. Main class: com.facebook.presto.hive.HiveExternalWorkerQueryRunner

   
NOTE:
   HiveExternalWorkerQueryRunner will basically launch a testing presto service using local file system.
   WORKER_COUNT is the number of workers to be launched along with the coordinator. In this case we put 0 as we want to externally launch our own CPP worker.
   Note discovery URI. Something like http://127.0.0.1:54557. Use the last discovery URI in the InteliJ logs
   
Upon running this you should see the Presto service log printing in the console. 

3. Update presto native worker configuration
The configuration structrue is stricly the same as Presto-java. And you can put the etc directory anywhere your like. 
::

   $ mkdir ${path-to-presto}/presto-native-execution/etc
   $ cd etc
   $ vim config.properties

Add the basic configuration for a presto worker. Use discovery URI from the logs above and update the config.properties. 
The config.properties file should be like: 
::

   task.concurrent-lifespans-per-task=32
   http-server.http.port=7071
   task.max-drivers-per-task=96
   discovery.uri=http://127.0.0.1:54557
   system-memory-gb=64
   presto.version=testversion

And then you need modify the node.properties
::

   $ vim node.properties

The node.properties should be like:
::

   node.id=3ae8d81c-97b8-42c4-9e49-3524cfbe5d8b
   node.ip=127.0.0.1
   node.environment=testing
   node.location=test-location

Then you need to modify the configuration for a catalog.
::

   $ mkdir catalog
   $ vim hive.properties

Note: You don't have to configure a real hive catalog.
In the HiveExternalWorkerQueryRunner it'll create a pseudo hive metastore for you. 

The hive.properties should be like:

::

   connector.name=hive

4. Launch presto native worker
   
Go to YOUR_PATH_TO_PRESTO_SERVER: 
::

   cd ${path-to-presto}/presto-native-execution/_build/release/presto_cpp/main/
   # launch the worker
   ./presto_server --v=1 --logtostderr=1 --etc_dir=${path-to-your-etc-directory}

When you see "Announcement succeeded: 202" printed to the console, the presto native worker has successfully connected to the coordinator. 

5. Test the queries
You can sent out queries using your existing presto-cli our go to the presto-cli module you just compiled.
::

   $ cd ${path-to-presto}/presto-cli/target
   $ ./presto-cli-${PRESTO_VERSION}-SNAPSHOT-executable.jar --catalog hive --schema tpch

By doing this you can launch an interactive SQL command.
Try Some queries with Prestissimo + BDTK!



Run a DEMO using HDFS
^^^^^^^^^^^^^^^^^^^^^^
*Note: The following steps should be done in the docker container*
| **Prerequisite:**
| A real Hadoop cluster with a running Hive metastore service. 

1. Install Kerberos
   You can skip this step if you've Kerberos installed on your env. 
   a. Download Kerberos from its website(http://web.mit.edu/kerberos/dist/)

::

   $ wget http://web.mit.edu/kerberos/dist/krb5/1.19/krb5-${krb5-version}.tar.gz
   $ tar zxvf krb5-${krb5-version}.tar
   $ cd krb5-${krb5-version}/src/include/krb5/krb5.hin krb5-${krb5-version}/src/include/krb5/krb5.h
   
1. Install the libraries for HDFS/S3
::

   # Set temp env variable for adaptors installation
   $ export KERBEROS_INCLUDE_DIRS=${path-to-krb}/src/include
   $ cd ${path-to-presto}/presto-native-execution/velox-plugin/ci/scripts
   # Run the script to set up for adpators
   $ ./setup-adapters.sh

2. Add specific flag when compiling presto_cpp
::

   # Make sure you have finished the BDTK integration before continuing
   $ cd ${path-to-presto}/presto-native-execution
   $ make PRESTO_ENABLE_PARQUET=ON VELOX_ENABLE_HDFS=ON debug

3. Launch a distributed Presto serivce
a. Launch your coordinator as normal presto-java server. 
You can find out how to launch a presto-java coorinator from here(https://prestodb.io/docs/current/installation/deployment.html)
b. Edit the configuration of presto native worker under your etc directory:
Modify ${path-to-presto-server-etc}/config.properties

::

   task.concurrent-lifespans-per-task=32
   http-server.http.port=9876
   task.max-drivers-per-task=96
   discovery.uri=${discovery-uri}
   system-memory-gb=64
   presto.version=${your-presto-version}

*NOTE: make sure the presto version is the same as your coordinator*
Modify ${path-to-presto-server-etc}/config.properties

::

   node.id=${your-presto-node-id}
   node.ip=${your-presto-node-ip}
   node.environment=${your-presto-env}
   node.location=test-location

Modify ${path-to-presto-server-etc}/catalog/hive.properties

::

   connector.name=hive-hadoop2
   hive.metastore.uri=thrift://${your-hive-metastore-serivce}
   hive.hdfs.host=${your-hdfs-host}
   hive.hdfs.port=${your-hdfs-port}

c. launch the presto native worker

:: 

   $ {path-to-presto}/presto-native-execution/_build/release/presto_cpp/main/presto_server --v=1 --logtostderr=1 --etc_dir=${path-to-your-etc-directory}

When you see "Announcement succeeded: 202" printed to the console, the presto native worker has successfully connected to the coordinator. 


Run with released package
^^^^^^^^^^^^^^^^^^^^^^^^^^
From the release note of BDTK: https://github.com/intel/BDTK/releases , you can download the package of presto_server binary file and libraries. 
You can directly run presto native worker with them to skip compiling step.

1. Unzip the package
   
::

   $ wget https://github.com/intel/BDTK/releases/download/${latest_tag}/bdtk_${latest_version}.tar.gz
   $ cd Prestodb

2. Prepare configuration files
   You need to prepare the basic configuration files as mentioned above. 

3. Launch presto native worker with binary file

:: 

   $ # add libraries to include path
   $ export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib
   $ # launch the server
   $ # --v=1 --logtostderr=1 are flags to print log, you can modify it as your wish
   $ ./bin/presto_server --v=1 --logtostderr=1 --etc_dir=${path-to-your-etc-directory}

When you see "Announcement succeeded: 202" printed to the console, the presto native worker has successfully connected to the coordinator. 
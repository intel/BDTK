How to contribute
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

How to debug/test
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

Code style check
------------------------
Code style check will be triggered automatically after you submit a PR. So please ensure your PR does not break any of these workflows. It runs ``make format-check``, ``make header-check`` as part of our continuous integration. 
Pull requests should pass ``format-check`` and ``header-check`` without errors before being accepted.

More details can be found at `ci/scripts/run_cpplint.py <https://github.com/intel/BDTK/blob/main/ci/scripts/run_cpplint.py>`_

Documentation
------------------------

BDTK documentation uses sphinx to produce html structure.
Github pages refer to "docs" directory on "gh-pages" branch.

**Build and commit**

We maintain gh-pages with github actions, which is implemented in .github/workflows/update-gh-pages.yml.

We can simply edit rst files under "docs" directory, when the change merge to "main" branch,
github action will automatically build gh-pages.

If you want to add a new rst file, remember add its title to "index.rst". 

**External links**

Last, share some tools and documents, hope it can help:

- Sphinx quick start: `sphinx-doc <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`_

- How to write rst(reStructuredText) files: `rst-tutorial <https://www.devdungeon.com/content/restructuredtext-rst-tutorial-0>`_

- Transfer markdown to rst: `md-to-rst <https://cloudconvert.com/md-to-rst>`_

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
   
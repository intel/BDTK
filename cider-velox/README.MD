# Introduction

This repo is target to be a JIT query engine plugin for Velox engine (and may support other query engine in the future). Currently, We use **OmnisciDB** as query codegen module, use **Substrait** as query plan protocol, use **Arrow C data interface** as data protocol.

# Architecture overview

To de added.

# Contributing
Before raising a PR, please run the following commands locally to make sure that your contribution follows the license and code style rules of this repo:  
* Please run `make header-fix-local` to fix license header
* Please run `make format-fix-local` to fix code style

Note: Please install the dependencies needed for PR check by running: `pip install regex black cmakelang`

# License

This repo is licensed under Apache 2.0 License.  

# Code style

The code style has been defined in `.clang-format`.  

# Notice

Scripts under script directory are copied from https://github.com/facebookincubator/velox. These scripts will check code style and license in order to keep consistency with velox.

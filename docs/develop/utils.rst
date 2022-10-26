======
Utils
======

Sql2IR
-----------------------------------

This tool can dump substrait plan json, llvm ir and cfg from a SQL.

How to use
```````````
Sql2IR is under cider/tests/ directory. follow the debugging guide, build all the test binary, and the Sql2IR tool is built.

you can find the usage by::
    
    Sql2IR --help

dump substrait plan from sql::
    
    Sql2IR --sql "select sum(a*b)  as res from A where a>10 and b<5" --create-ddl "create table A(a int, b int);"

If you want dump the IR cfg, dump-ir-level should be set to 2::

    Sql2IR --sql "select sum(a*b)  as res from A where a>10 and b<5" --create-ddl "create table A(a int, b int);" --dump-ir-level=2 --gen-cfg=true


Logical and Comparsion ops
=================================

**logical operators**

support logical operators(and, or, not)


**comparsion operators**

support basic comparsion operators(<,>, <=, >=, =, <>, !=)

**between (x) and (y)**

The between operator tests if a value is within a specified range. 

**is null / is not null**

is null and is not null operators test whether a value is null. Both operators work for all data types.


**is distinct from / is not ditinct from**

a null value is not considered distinct from NULL. When you are comparing values which may include NULL use these operators to guarantee either a TRUE or FALSE result.

.. code-block:: sql

    SELECT NULL IS DISTINCT FROM NULL; -- false

    SELECT NULL IS NOT DISTINCT FROM NULL; -- true

**like**

The like operator is used to match a specified character pattern in a string. 


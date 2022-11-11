Conditional Expression
==========================================
**case**

The standard SQL CASE expression has two forms. The “simple” form searches each value expression from left to right until it finds one that equals expression:

.. code-block:: sql

    CASE expression
        WHEN value THEN result
        [ WHEN ... ]
        [ ELSE result ]
    END

**if(condition, true_value, false_value)**

Evaluates and returns true_value if condition is true, otherwise evaluates and returns false_value.

**coalesce(value1, value2[, ...])**

Returns the first non-null value in the argument list.

Conditional Expressions in Cider
-----------------------------------
COALESCE
^^^^^^^^^^^^^
The COALESCE expression is a syntactic shortcut for the CASE expression

The code COALESCE(expression1,...n) is executed in Cider as the following CASE expression:

.. code-block:: sql

        CASE  
        WHEN (expression1 IS NOT NULL) THEN expression1  
        WHEN (expression2 IS NOT NULL) THEN expression2  
        ...  
        ELSE expressionN  
        END

Example: 

.. code-block:: sql

        SELECT COALESCE(col_1, col_2, 777) FROM test


is equal to

.. code-block:: sql

        SELECT CASE WHEN col_1 is not null THEN col_1 WHEN col_2 is not null THEN col_2 ELSE 777 END from test


IF
^^^^^^
The IF function is actually a language construct that is executed in Cider as the following CASE expression

.. code-block

:: 

        CASE
        WHEN condition THEN true_value
        [ ELSE false_value ]
        END

IF Functions: 

.. code-block

:: 

        if(condition, true_value)

Evaluates and returns true_value if condition is true, otherwise null is returned and true_value is not evaluated.

is equal to

.. code-block:: sql

        CASE WHEN condition THEN true_value END

.. code-block

:: 

        if(condition, true_value, false_value)

Evaluates and returns true_value if condition is true, otherwise evaluates and returns false_value.

is equal to

.. code-block:: sql

        CASE WHEN condition THEN true_value ELSE false_value END


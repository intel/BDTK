Function support
=====================

Aggregate Functions
--------------------------------------
**avg(x)**

Returns the average (arithmetic mean) of all input values.

**sum(x)**

Returns the sum of all input values.

**count(x)**

Returns the number of non-null input values.

**max(x)**

Returns the maximum value of all input values

**min(x)**

Returns the minimum value of all input values.


Logical Ops and Comparsion Functions
--------------------------------------

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

Conversion Functions
--------------------------------------

**cast(expr as type)**

Explicitly cast a value as a type. This can be used to cast a varchar to a numeric value type.


Time Functions
--------------------------------------
**extract(unit from date)**

returns one or more separate parts of a date or time in the specified unit. For example, this function can return the year, month, day, hour, or minute of a date or time.

**time ± intervals**

Adds an interval value of type unit to time type. Subtraction can be performed by using a negative value.


Conditional Expression Functions
--------------------------------------
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


Mathematical Functions
--------------------------------------

**mathematical operators**

`support Mathematical Operators(+, -, *, /, %)`


String Functions
--------------------------------------

**substr(string, start, length)**

Returns a substring from string of length from the starting position start.

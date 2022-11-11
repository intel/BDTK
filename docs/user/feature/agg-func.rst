Aggregate Functions
=================================

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

AVG support in Cider
-----------------------------------

Similar as other aggregation functions, 'AVG' has 2 phases(Partial/Final) in distributing data analytic engines. But computation is different in different phase. In AVG partial, computation is split into sum() and count() on target column/expression and in AVG final, sum() is done on previous summation and count value, then do a divide between these 2 values.

Since Cider is positioned as a compute library under such a distributed engine at task level, it doesn't support AVG syntax directly in its internal.

It may have some conflictions when frontend framework offloads AVG function to Cider, mainly caused by different signature of referred functions, such as output type, etc. Take Velox for example, it specifies **sum(int)** with output type **double** in avg aggregation, while it violates rules in cider which uses output type **bigint**. This will cause codegen check failure. So for this case, we made a workaround by following Cider rules in internal and convert result to **double** when retriving result into CiderBatch, thus can keep consistent schema with following op in velox plan, such as avg final computation.

Similar special handle will be needed when output type of agg functions from frontend framework violates with cider internal. In cider, the returned data types defined as following:

.. list-table
   :widths: 10 30
   :align: left
   :header-rows: 1
::

   * - Aggregate Function
     - Output Type
   * - SUM
     - If argument is integer, output type will be BIGINT. Otherwise same as argument type.
   * - MIN
     - Same as argument type.
   * - MAX
     - Same as argument type.
   * - COUNT
     - If g_bigint_count is true(default false), output type is BIGINT. Otherwise uses INT.
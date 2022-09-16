=================================
Having syntax support in Cider
=================================

The HAVING clause was added to SQL because the WHERE keyword cannot be used with aggregate functions.
WHERE filters at the record level, while HAVING often filters at the "group of records" level or GROUPBY keys.

For example:

.. code-block:: sql

        SELECT fx, COUNT(*) FROM test GROUP BY fx HAVING COUNT(*) > 5


.. code-block:: sql

    	SELECT x, MAX(z) FROM test WHERE z IS NOT NULL GROUP BY x HAVING x > 7

Since Cider works as a compute library, the plan it received shoule be already parsed and thus HAVING syntax
is not directly consumed. For above 2 scenarios, the HAVING clause takes effect as a filter node
after one final aggregation node, like pattern:

.. code-block::

        filterproject <- Aggregate(Final) ......... Aggregate(Partial) <- filterproject <- tablescan

And in having-subquery scenarios, for example: 

.. code-block:: sql

        SELECT x FROM test GROUP BY x HAVING avg(y) > (select avg(y) from test);  

the HAVING clause takes effect as a filter node after one cross join node, like pattern:

.. code-block::

        filterproject <- CrossJoin <- Project <- Aggregate(Final) ......... Aggregate(Partial) <- tablescan
                                       ......... Aggregate(Final) ......... Aggregate(Partial) <- tablescan

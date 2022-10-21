===================
Cider in 10 minutes
===================

Introduction
--------------------------------------

This is a quick introduction into Cider, ...

APIs
--------------------------------------

The following table shows the query parameters for this service.

=================== ==================================== ========
Attribute                      Description               Required
=================== ==================================== ========
CiderRuntimeModule	   The runtime module of Cider	        Yes
=================== ==================================== ========

Runtime Module
++++++++++++++++++++++

The following attributes will be used in your development:

* ``getGroupByAggHashTableIteratorAt``

  Return the iterator pointer of the input offset.

* ``convertGroupByAggHashTableToString``

  Convert group by agg hash table to string (For test only).

* ``convertQueryMemDescToString``

  Convert query memory descriptor to string (For test only).


Code Blocks
++++++++++++++++++++++
.. code-block:: c++

  // Group-by Agg related functions
  CiderAggHashTableRowIteratorPtr getGroupByAggHashTableIteratorAt(size_t index);
  const std::string convertGroupByAggHashTableToString() const;  // For test only
  const std::string convertQueryMemDescToString() const;         // For test only
  size_t getGroupByAggHashTableBufferNum() const;
  bool isGroupBy() const;

Useful Documents
--------------------------------------
`How to write rst <https://pythonhosted.org/an_example_pypi_project/sphinx.html>`_


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

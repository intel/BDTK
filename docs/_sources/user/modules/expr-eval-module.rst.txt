Expression Evaluation Module Introduction
==========================================

Module descritption
-------------------------

Expression evaluation module could do computation for Project/Filter expressions effectively. It provides a runtime expression evaluation API which accepts Substrait based expression representation and Apache Arrow based column format data representation.

SDK API
--------------------------

.. code-block:: cpp

   // validate expression can be evaluated by Cider
   bool is_supported = ExpressionValidator::validate(extended_expression);
   // compile and prepare runtime context
   auto expr_evaluator = std::make_shared<ExpressionEvaluator>(extended_expression, std::make_shared<ExprEvaluatorContext>(allocator));
   // return computation result in arrow-array format
   struct ArrowArray output_array;
   struct ArrowSchema output_schema;
   expr_evaluator::eval(input_array, input_schema, output_array, output_schema);
   // schema check API for input array
   bool is_matched = expr_evaluator::schemaCheck(input_schema, substrait_schema);

Please refer `ExpressionEvalExample <https://github.com/intel/BDTK/blob/main/cpp/src/cider/examples/ExpressionEvalExample.cpp>`__ for detailed usage.

Supported functions
--------------------------

Here is a list of all scalar functions supported in expression evaluation.

.. table::
    :widths: auto
    :class: rows

    ======================  ======================  ======================  ======================  ======================  ======================
    Scalar Functions
    ==============================================================================================================================================
    plus                    minus                   multiply                devide                  mod                     gt                  
    lt                      equal                   not_equal               gte                     lte                     cast                
    in                      is_distinct_from        is_not_distinct_from    is_null                 is_not_null             and
    or                      not                     lower                   upper                   trim                    substring             
    regexp_replace          regexp_substring        char_length             ltrim                   rtrim                   split                 
    extract                 concat                  like                    regex_like
    ======================  ======================  ======================  ======================  ======================  ======================

For other SQL syntax support, like case...when..., coalesce, IF, time ± intervals, etc, you can refer details under `feature <https://github.com/yma11/BDTK/tree/doc/docs/user/feature>`__.

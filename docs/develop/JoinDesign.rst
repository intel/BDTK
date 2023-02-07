==========================
HashJoin Translator design
==========================

API Introduction
--------------------------------------

void HashJoinTranslator::codegen() 
++++++++++++++++++++++++++++++++++++++

1. get join_quals, join_type, build_table_map from HashJoinNode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
build_table_map:std::map<ExprPtr, size_t>, record its expr and offset in batch


2. traverse join_quals to get join_key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
void traverse(..): traverse join_quals expr tree to get join key value and null vector

3. pack join_key_value, join_key_null (maybe multi key)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

4. register buffer(join_res_buffer) to reserve the join result
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

5. register hashTable (hashtable)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

6. call look_up_value_by_key function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
get the join result(store the result in join_res_buffer, return result rows)  
join result type:vector<Batch*, batch_offset>

7. loop_builder(the loop to traverse join results, row_id as the index)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
switch(join_type)
  
 - `7.1 inner_join`:
  
 ::

     for(row_index = 0; row_index < join_res_len; row_index++){ 
         call join_probe() (as introduction below)
     }

 - `7.1 left_join`:

 First, maintain the number of cycles through row_index, first set the value of the expr to the default value (value = 0, null=true), 
 if join_res_len = 0, that is the data in the probe table but not in the build table, take the default value, Jump out of the loop and enter the next op, 
 if it is not 0, then run joinProbe() to get the corresponding real value

 ::

     if(join_res_len != 0) { row_index++; }
     for(row_index; row_index<=join_res_len;row_index++){
         for(auto item: build_table_map.exprs){
             setDefaultValue(for left join, if join_res_len == 0, also needs output);
         }
         if(join_res_len == 0){ 
             continue; 
         }
         joinProbe();
     }

in ``ExprDefaultValueSetter``, the variable ``std::map<ExprPtr, std::vector<JITValuePointer>>& expr_map_`` is used to preseve
the expr and its corresponding default buffer value, Note that we used ``createVariable()``, it will work in ``joinProbe()`` JoinType::LEFT

 ::
    
     expr_map_.insert({expr_, {null_init, val_init}});

void joinProbe()
++++++++++++++++++++++++++++++++++++++

1. get res_array(batch*), res_row_id(batch_offset) from join_res_buffer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

2. traverse build_table_map to get the array->child, and get multi buffer value
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

3. BuildTableReader reader()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
use the BuildTableReader to to populate the value of the corresponding column of the build table, BuildTableReader code structure is similar to ColumnToRowReader

4. join_type == JoinType::LEFT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
for Left join type, we created the continue LLVM BasicBlock.
and in LLVM code, two parallel basicBlock cannot call each other's internally defined local variables,
so the pre-defined variables are used here to get the result after read and save them as variables

 ::
    
     vector[i] = *expr_val[i];
     expr_val[i].replace(vector[i]);

Notes
--------------------------------------

will add later
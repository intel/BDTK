-- Copyright(c) 2022-2023 Intel Corporation.
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
WITH results AS
  (SELECT i_product_name ,
          i_brand ,
          i_class ,
          i_category ,
          inv_quantity_on_hand qoh
   FROM inventory ,
        date_dim ,
        item ,
        warehouse
   WHERE inv_date_sk=d_date_sk
     AND inv_item_sk=i_item_sk
     AND inv_warehouse_sk = w_warehouse_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11 ),
     results_rollup AS
  (SELECT i_product_name,
          i_brand,
          i_class,
          i_category,
          avg(qoh) qoh
   FROM results
   GROUP BY i_product_name,
            i_brand,
            i_class,
            i_category
   UNION ALL SELECT i_product_name,
                    i_brand,
                    i_class,
                    NULL i_category,
                         avg(qoh) qoh
   FROM results
   GROUP BY i_product_name,
            i_brand,
            i_class
   UNION ALL SELECT i_product_name,
                    i_brand,
                    NULL i_class,
                         NULL i_category,
                              avg(qoh) qoh
   FROM results
   GROUP BY i_product_name,
            i_brand
   UNION ALL SELECT i_product_name,
                    NULL i_brand,
                         NULL i_class,
                              NULL i_category,
                                   avg(qoh) qoh
   FROM results
   GROUP BY i_product_name
   UNION ALL SELECT NULL i_product_name,
                         NULL i_brand,
                              NULL i_class,
                                   NULL i_category,
                                        avg(qoh) qoh
   FROM results)
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       qoh
FROM results_rollup
ORDER BY qoh NULLS FIRST,
         i_product_name NULLS FIRST,
         i_brand NULLS FIRST,
         i_class NULLS FIRST,
         i_category NULLS FIRST
LIMIT 100;


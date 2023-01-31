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
  (SELECT i_item_id,
          s_state,
          0 AS g_state,
          ss_quantity agg1,
          ss_list_price agg2,
          ss_coupon_amt agg3,
          ss_sales_price agg4
   FROM store_sales,
        customer_demographics,
        date_dim,
        store,
        item
   WHERE ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk
     AND ss_store_sk = s_store_sk
     AND ss_cdemo_sk = cd_demo_sk
     AND cd_gender = 'M'
     AND cd_marital_status = 'S'
     AND cd_education_status = 'College'
     AND d_year = 2002
     AND s_state = 'TN' )
SELECT i_item_id,
       s_state,
       g_state,
       agg1,
       agg2,
       agg3,
       agg4
FROM
  ( SELECT i_item_id,
           s_state,
           0 AS g_state,
           avg(agg1) agg1,
           avg(agg2) agg2,
           avg(agg3) agg3,
           avg(agg4) agg4
   FROM results
   GROUP BY i_item_id ,
            s_state
   UNION ALL SELECT i_item_id,
                    NULL AS s_state,
                    1 AS g_state,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4
   FROM results
   GROUP BY i_item_id
   UNION ALL SELECT NULL AS i_item_id,
                    NULL AS s_state,
                    1 AS g_state,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4
   FROM results ) foo
ORDER BY i_item_id NULLS FIRST,
         s_state NULLS FIRST
LIMIT 100;

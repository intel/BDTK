-- Copyright (c) 2022 Intel Corporation.
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
  (SELECT sum(ss_net_profit) AS ss_net_profit,
          sum(ss_ext_sales_price) AS ss_ext_sales_price,
          (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin ,
          i_category ,
          i_class ,
          0 AS g_category,
          0 AS g_class
   FROM store_sales ,
        date_dim d1 ,
        item ,
        store
   WHERE d1.d_year = 2001
     AND d1.d_date_sk = ss_sold_date_sk
     AND i_item_sk = ss_item_sk
     AND s_store_sk = ss_store_sk
     AND s_state ='TN'
   GROUP BY i_category,
            i_class) ,
     results_rollup AS
  (SELECT gross_margin,
          i_category,
          i_class,
          0 AS t_category,
          0 AS t_class,
          0 AS lochierarchy
   FROM results
   UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                i_category,
                NULL AS i_class,
                0 AS t_category,
                1 AS t_class,
                1 AS lochierarchy
   FROM results
   GROUP BY i_category
   UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                NULL AS i_category,
                NULL AS i_class,
                1 AS t_category,
                1 AS t_class,
                2 AS lochierarchy
   FROM results)
SELECT gross_margin,
       i_category,
       i_class,
       lochierarchy,
       rank() OVER ( PARTITION BY lochierarchy,
                                  CASE
                                      WHEN t_class = 0 THEN i_category
                                  END
                    ORDER BY gross_margin ASC) AS rank_within_parent
FROM results_rollup
ORDER BY lochierarchy DESC NULLS FIRST,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;


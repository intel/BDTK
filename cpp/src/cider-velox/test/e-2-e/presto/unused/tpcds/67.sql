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
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id ,
          sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
   FROM store_sales,
        date_dim,
        store,
        item
   WHERE ss_sold_date_sk=d_date_sk
     AND ss_item_sk=i_item_sk
     AND ss_store_sk = s_store_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11
   GROUP BY i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy,
            s_store_id) ,
     results_rollup AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales
   FROM results
   UNION ALL SELECT i_category,
                    i_class,
                    i_brand,
                    i_product_name,
                    d_year,
                    d_qoy,
                    d_moy,
                    NULL s_store_id,
                         sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy
   UNION ALL SELECT i_category,
                    i_class,
                    i_brand,
                    i_product_name,
                    d_year,
                    d_qoy,
                    NULL d_moy,
                         NULL s_store_id,
                              sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy
   UNION ALL SELECT i_category,
                    i_class,
                    i_brand,
                    i_product_name,
                    d_year,
                    NULL d_qoy,
                         NULL d_moy,
                              NULL s_store_id,
                                   sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year
   UNION ALL SELECT i_category,
                    i_class,
                    i_brand,
                    i_product_name,
                    NULL d_year,
                         NULL d_qoy,
                              NULL d_moy,
                                   NULL s_store_id,
                                        sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class,
            i_brand,
            i_product_name
   UNION ALL SELECT i_category,
                    i_class,
                    i_brand,
                    NULL i_product_name,
                         NULL d_year,
                              NULL d_qoy,
                                   NULL d_moy,
                                        NULL s_store_id,
                                             sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class,
            i_brand
   UNION ALL SELECT i_category,
                    i_class,
                    NULL i_brand,
                         NULL i_product_name,
                              NULL d_year,
                                   NULL d_qoy,
                                        NULL d_moy,
                                             NULL s_store_id,
                                                  sum(sumsales) sumsales
   FROM results
   GROUP BY i_category,
            i_class
   UNION ALL SELECT i_category,
                    NULL i_class,
                         NULL i_brand,
                              NULL i_product_name,
                                   NULL d_year,
                                        NULL d_qoy,
                                             NULL d_moy,
                                                  NULL s_store_id,
                                                       sum(sumsales) sumsales
   FROM results
   GROUP BY i_category
   UNION ALL SELECT NULL i_category,
                         NULL i_class,
                              NULL i_brand,
                                   NULL i_product_name,
                                        NULL d_year,
                                             NULL d_qoy,
                                                  NULL d_moy,
                                                       NULL s_store_id,
                                                            sum(sumsales) sumsales
   FROM results)
SELECT *
FROM
  (SELECT i_category ,
          i_class ,
          i_brand ,
          i_product_name ,
          d_year ,
          d_qoy ,
          d_moy ,
          s_store_id ,
          sumsales ,
          rank() OVER (PARTITION BY i_category
                       ORDER BY sumsales DESC) rk
   FROM results_rollup) dw2
WHERE rk <= 100
ORDER BY i_category NULLS LAST,
         i_class NULLS LAST,
         i_brand NULLS LAST,
         i_product_name NULLS LAST,
         d_year NULLS LAST,
         d_qoy NULLS LAST,
         d_moy NULLS LAST,
         s_store_id NULLS LAST,
         sumsales NULLS LAST,
         rk NULLS LAST
LIMIT 100;


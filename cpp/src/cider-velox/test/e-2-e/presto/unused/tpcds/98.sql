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
SELECT i_item_id ,
       i_item_desc,
       i_category,
       i_class,
       i_current_price ,
       sum(ss_ext_sales_price) AS itemrevenue,
       sum(ss_ext_sales_price)*100.0000/sum(sum(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales ,
     item,
     date_dim
WHERE ss_item_sk = i_item_sk
  AND i_category IN ('Sports',
                     'Books',
                     'Home')
  AND ss_sold_date_sk = d_date_sk
  AND d_date BETWEEN cast('1999-02-22' AS date) AND cast('1999-03-24' AS date)
GROUP BY i_item_id ,
         i_item_desc,
         i_category ,
         i_class ,
         i_current_price
ORDER BY i_category  NULLS FIRST,
         i_class  NULLS FIRST,
         i_item_id  NULLS FIRST,
         i_item_desc  NULLS FIRST,
         revenueratio NULLS FIRST;

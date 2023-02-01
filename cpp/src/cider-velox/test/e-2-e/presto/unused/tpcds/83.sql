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
WITH sr_items AS
  (SELECT i_item_id item_id,
          sum(sr_return_quantity) sr_item_qty
   FROM store_returns,
        item,
        date_dim
   WHERE sr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM date_dim
             WHERE d_date IN ('2000-06-30',
                              '2000-09-27',
                              '2000-11-17')))
     AND sr_returned_date_sk = d_date_sk
   GROUP BY i_item_id),
     cr_items AS
  (SELECT i_item_id item_id,
          sum(cr_return_quantity) cr_item_qty
   FROM catalog_returns,
        item,
        date_dim
   WHERE cr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM date_dim
             WHERE d_date IN ('2000-06-30',
                              '2000-09-27',
                              '2000-11-17')))
     AND cr_returned_date_sk = d_date_sk
   GROUP BY i_item_id),
     wr_items AS
  (SELECT i_item_id item_id,
          sum(wr_return_quantity) wr_item_qty
   FROM web_returns,
        item,
        date_dim
   WHERE wr_item_sk = i_item_sk
     AND d_date IN
       (SELECT d_date
        FROM date_dim
        WHERE d_week_seq IN
            (SELECT d_week_seq
             FROM date_dim
             WHERE d_date IN ('2000-06-30',
                              '2000-09-27',
                              '2000-11-17')))
     AND wr_returned_date_sk = d_date_sk
   GROUP BY i_item_id)
SELECT sr_items.item_id ,
       sr_item_qty ,
       (sr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 sr_dev ,
       cr_item_qty ,
       (cr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 cr_dev ,
       wr_item_qty ,
       (wr_item_qty*1.0000)/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0000 * 100 wr_dev ,
       (sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
FROM sr_items ,
     cr_items ,
     wr_items
WHERE sr_items.item_id=cr_items.item_id
  AND sr_items.item_id=wr_items.item_id
ORDER BY sr_items.item_id NULLS FIRST,
         sr_item_qty NULLS FIRST
LIMIT 100;


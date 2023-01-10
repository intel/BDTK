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

-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
select
  p.brand,
  p.type,
  p.size,
  count(distinct ps.suppkey) as supplier_cnt
from
  partsupp as ps,
  part as p
where
  p.partkey = ps.partkey
  and p.brand <> 'Brand#45'
  and p.type not like 'MEDIUM POLISHED%'
  and p.size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps.suppkey not in (
    select
      suppkey
    from
      supplier
    where
      comment like '%Customer%Complaints%'
  )
group by
  p.brand,
  p.type,
  p.size
order by
  supplier_cnt desc,
  p.brand,
  p.type,
  p.size;

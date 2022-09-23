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

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
select
	ps.partkey,
	sum(ps.supplycost * ps.availqty) as value
from
	partsupp as ps,
	supplier as s,
	nation as n
where
	ps.suppkey = s.suppkey
	and s.nationkey = n.nationkey
	and n.name = 'GERMANY'
group by
	ps.partkey
having
    sum(ps.supplycost * ps.availqty) > (
        select
            sum(ps.supplycost * ps.availqty) * 0.0001
        from
            partsupp as ps,
            supplier as s,
            nation as n
        where
            ps.suppkey = s.suppkey
            and s.nationkey = n.nationkey
            and n.name = 'GERMANY'
    )
order by value desc;

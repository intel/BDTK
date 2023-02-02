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

-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
select
	c.custkey,
	c.name,
	sum(l.extendedprice * (1 - l.discount)) as revenue,
	c.acctbal,
	n.name,
	c.address,
	c.phone,
	c.comment
from
	customer as c,
	orders as o,
	lineitem as l,
	nation as n
where
	c.custkey = o.custkey
	and l.orderkey = o.orderkey
	and o.orderdate >= date '1993-10-01'
	and o.orderdate < date '1993-10-01' + interval '3' month
	and l.returnflag = 'R'
	and c.nationkey = n.nationkey
group by
	c.custkey,
	c.name,
	c.acctbal,
	c.phone,
	n.name,
	c.address,
	c.comment
order by
	revenue desc
limit 20;

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

-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n.name as nation,
			extract(year from date(o.orderdate)) as o_year,
			l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity as amount
		from
			part as p,
			supplier as s,
			lineitem as l,
			partsupp as ps,
			orders as o,
			nation as n
		where
			s.suppkey = l.suppkey
			and ps.suppkey = l.suppkey
			and ps.partkey = l.partkey
			and p.partkey = l.partkey
			and o.orderkey = l.orderkey
			and s.nationkey = n.nationkey
			and p.name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;

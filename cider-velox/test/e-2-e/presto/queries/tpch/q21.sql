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

-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- Functional Query Definition
-- Approved February 1998
select
	s.name,
	count(*) as numwait
from
	supplier as s,
	lineitem as l1,
	orders as o,
	nation as n
where
	s.suppkey = l1.suppkey
	and o.orderkey = l1.orderkey
	and o.orderstatus = 'F'
	and l1.receiptdate > l1.commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.orderkey = l1.orderkey
			and l2.suppkey <> l1.suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.orderkey = l1.orderkey
			and l3.suppkey <> l1.suppkey
			and l3.receiptdate > l3.commitdate
	)
	and s.nationkey = n.nationkey
	and n.name = 'SAUDI ARABIA'
group by
	s.name
order by
	numwait desc,
	s.name
limit 100;

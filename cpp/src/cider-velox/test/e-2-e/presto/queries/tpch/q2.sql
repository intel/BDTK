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

-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998
select
	s.acctbal,
	s.name,
	n.name,
	p.partkey,
	p.mfgr,
	s.address,
	s.phone,
	s.comment
from
	part as p,
	supplier as s,
	partsupp as ps,
	nation as n,
	region as r
where
	p.partkey = ps.partkey
	and s.suppkey = ps.suppkey
	and p.size = 15
	and p.type like '%BRASS'
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'EUROPE'
	and ps.supplycost = (
		select
			min(ps.supplycost)
		from
			partsupp as ps,
			supplier as s,
			nation as n,
			region as r
		where
			p.partkey = ps.partkey
			and s.suppkey = ps.suppkey
			and s.nationkey = n.nationkey
			and n.regionkey = r.regionkey
			and r.name = 'EUROPE'
	)
order by
	s.acctbal desc,
	n.name,
	s.name,
	p.partkey
limit 100;

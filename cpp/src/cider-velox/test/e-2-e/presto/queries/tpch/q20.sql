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

-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
select
	s.name,
	s.address
from
	supplier as s,
	nation as n
where
	s.suppkey in (
		select
            ps.suppkey
		from
			partsupp as ps
		where
			ps.partkey in (
				select
					partkey
				from
					part
				where
					name like 'forest%'
			)
			and ps.availqty > (
				select
					0.5 * sum(l.quantity)
				from
					lineitem as l
				where
					l.partkey = ps.partkey
					and l.suppkey = ps.suppkey
					and l.shipdate >= date('1994-01-01')
					and l.shipdate < date('1994-01-01') + interval '1' year
			)
	)
	and s.nationkey = n.nationkey
	and n.name = 'CANADA'
order by
	s.name;

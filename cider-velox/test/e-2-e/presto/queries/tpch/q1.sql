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

-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
-- Fixed shipdate predicate as DWRF doesn't support DATE types
select
	returnflag,
	linestatus,
	sum(quantity) as sum_qty,
	sum(extendedprice) as sum_base_price,
	sum(extendedprice * (1 - discount)) as sum_disc_price,
	sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
	avg(quantity) as avg_qty,
	avg(extendedprice) as avg_price,
	avg(discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
    shipdate >= date '1998-12-01' - interval '90' day
    and shipdate <= date '1998-12-01'
group by
	returnflag,
	linestatus
order by
	returnflag,
	linestatus;

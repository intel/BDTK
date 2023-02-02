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

--orderby
select
        *
from
        lineitem
order by
        orderkey desc nulls last;

select
        *
from
        lineitem
order by
        orderkey nulls first,
        shipmode nulls last;

--limit
select
        *
from
        lineitem
limit 10;

-- disable this since presto native not support offset
-- select
--         *
-- from
--         lineitem
-- offset 5
-- limit 10;

--top-n
select
        *
from
        lineitem
order by
        orderkey nulls first
limit 10;

select
        *
from
        lineitem
where
        orderkey > 15
order by
        orderkey nulls first
limit 10;

select
        *
from
        lineitem
where
        orderkey > 15
order by
        orderkey nulls first,
        shipmode desc nulls last
limit 10;


select
        *
from
        lineitem
where
        orderkey > 15
order by
        partkey + 1
limit
        10;

select
*
from
        lineitem
where
        orderkey > 15
order by
        1
limit
        10;

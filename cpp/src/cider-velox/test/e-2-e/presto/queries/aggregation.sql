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

-- average 
select 
        orderkey, 
        linenumber, 
        avg(quantity) as avg_price 
from 
        lineitem 
where       
        shipdate < '24.0'
group by 
        orderkey, 
        linenumber;

-- Agg Op groupby_multi_col
-- bigint VARCHAR double 
-- select 
--         orderkey, 
--         shipdate, 
--         count(*), 
--         sum(extendedprice * discount) as revenue, 
--         sum(quantity) as sum_quan 
-- from   
--         lineitem 
-- group by 
--         orderkey, shipdate;

-- Agg Op-groupby_single_col
select 
        orderkey, 
        count(discount),
        sum(extendedprice * discount) as revenue, 
        sum(quantity) as sum_quan 
from 
        lineitem
group by 
        orderkey;

-- Agg having_agg
-- Enable this after POAE7-1754 merged. 
-- PlanUtil cloneNodeWithNewSource more support.
-- select 
--         orderkey, 
--         sum(quantity) as sum_quan 
-- from 
--         lineitem 
-- group by 
--         orderkey
-- having 
--         sum(quantity) > 2.0;

select 
        linenumber, 
        sum(linenumber) as sum_linenum 
from 
        lineitem 
group by 
        linenumber 
having 
        linenumber > 2;

-- Agg having and where
select 
        linenumber, 
        sum(linenumber) as sum_a 
from 
        lineitem 
where 
        linenumber < 10 
group by 
        linenumber 
having 
        linenumber > 2;

-- Enable this after POAE7-1754 merged. 
-- PlanUtil cloneNodeWithNewSource more support.
-- select 
--         linenumber, 
--         sum(linenumber) as sum_a 
-- from 
--         lineitem 
-- where 
--         linenumber < 10 
-- group by 
--         linenumber 
-- having 
--         sum(linenumber) > 2;

-- Agg having groupby multi cols
select 
        orderkey, 
        partkey, 
        quantity, 
        sum(orderkey), 
        sum(partkey), 
        sum(quantity) 
from            
        lineitem                                                                     
group by 
        orderkey, 
        partkey, 
        quantity 
having 
        orderkey > 5 and partkey > 5;
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

-- verify mathematical operators: + - * / on double type
-- support for column with constant type and column with column
select
    -- col + constant
    totalprice + 0.234,
    totalprice - 0.234,
    totalprice * 0.234,
    totalprice / 0.234,
    -- col + col
    totalprice + shippriority,
    totalprice - shippriority,
    totalprice * shippriority,
    totalprice / shippriority
from
    orders;

-- verify mathematical operators: + - * / on bigint/int type
-- multiply op not supported for BIGINT type due to overflow multiplication/addition of INT64
-- select
--     -- col + constant
--     orderkey + 2,
--     orderkey - 2,
--     orderkey * 2,
--     orderkey / 2,
--     shippriority + 2,
--     shippriority - 2,
--     shippriority * 2,
--     shippriority / 2,
--     -- col + col
--     orderkey + shippriority,
--     orderkey - shippriority,
--     orderkey / case shippriority when 0 then 1 else shippriority end
-- from
--     orders;

-- verify mathematical operators: % on bigint type
select
    orderkey % 2,
    shippriority % 2
from
    orders;

--verify logical operators: and/or with null
select
    cast(null as boolean) and true,
    cast(null as boolean) and false,
    cast(null as boolean) or true,
    cast(null as boolean) or false
from
    orders;

--verify logical operators: and/or with not null
select
    linenumber = 2 and quantity >10,
    linenumber = 2 or quantity >10
from
    lineitem;

--verify logical operators: not with null
select
    not cast(null as boolean)
from
    lineitem;

--verify logical operator: not with not null
select
    *
from
    lineitem
where
    not is_open;

--verify logical function with basic compare ops
select
    3 < 2,
    3 > 2,
    3 = 2,
    3 <= 2,
    3 >= 2,
    3 <> 2
from
    lineitem;

--verify constant expression
select
    1,
    true
from
    lineitem
where
    linenumber = 2;

-- String Test
select
        name
from
        customer;

select
        address,
        name
from
        customer;

select
        *
from
        customer;

select
        name
from
        customer
where
        name = 'Customer#000000909';

select
        name
from
        customer
where
        name = 'Customer#000000928';

select
        name
from
        customer
where
        name <> '0000000000';

select
        address
from
        customer
where
        name <> '1111111111';

select
        address,
        name
from
        customer
where
        name <> '2222222222';

select
        *
from
        customer
where
        name <> 'aaaaaaaaaaa';

select
        *
from
        customer
where
        name <> 'abcdefghijklmn';

-- scalar function like : varchar
select
        name
from
        customer
where
        name like '%00894';

select
        name
from
        customer
where
        name like 'Customer#0000002%';

select
        name
from
        customer
where
        name like '%Customer#000000299%';

select
        name
from
        customer
where
        name like '%123%4';

select
        name
from
        customer
where
        name like '22%22';

select
        name
from
        customer
where
        name like '_ustomer#0000003%';

select
        name
from
        customer
where
        name like '44_%';

select
        name
from
        customer
where
        name like 'Customer#0000003%'
        or name like '%00';

select
        name
from
        customer
where
        name like 'Customer#0000003%'
        and name like '%00';
select
        name
from
        customer
where
        name like '_1111';

select
        name
from
        customer
where
        name not like '1111_';

select
        name
from
        customer
where
        name not like '44_4444444';

select
        name
from
        customer
where
        name not like '44_4%'
        and name not like '%111%';

--- Scalar Function like  :eg,varchar(20)
select
        address,
        name
from
        customer
where
        name <> '2222222222';

select
        comment
from
        customer
where
        comment not like 'bli_l%'
        and comment not like '%ack%';

select
        name
from
        customer
where
        name like '7777%'
        and name like '%8888';

select
        name
from
        customer
where
        name like '7777%'
        or name like '%8888';

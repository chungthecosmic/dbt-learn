{{ config(
    materialized="view",
    tags=["price","daily"],
    meta={
        "maturity": "develop",
        "owner": "teamA"
    }
) }}

with _source_a as (
    select *
    from {{ source("main", "price_daily1") }}
)
, _source_b as (
    select *
    from {{ source("main", "price_daily2") }}
)
select 
    ticker
    , date_kst
    , median(open) as open
    , median(high) as high
    , median(low) as low
    , median(close) as close
    , median(volume) as volume
from (
    select *
    from _source_a
    union all
    select * 
    from _source_b
) t
where volume != 0
group by 1,2

union all
select null as ticker
    , '2025-01-01' as date_kst
    , 0 as open
    , 0 as high
    , 0 as low
    , 0 as close
    , 0 as volume
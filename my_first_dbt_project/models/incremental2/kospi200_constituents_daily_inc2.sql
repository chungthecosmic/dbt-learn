{{
    config(
        materialized='incremental',
        incremental_strategy='delete_insert',
        unique_key=['target_index_name','ticker','date_kst']
    )
}}

select target_index_name
    , ticker
    , name
    , date_kst
from {{ source("main", "index_constituents_inc") }}
where 
    target_index_name = '코스피 200'
    and date_kst >= '{{ var("start_dt") }}'
    and date_kst <= '{{ var("end_dt") }}'


-- dbt run -s kospi200_constituents_daily_inc2 --vars '{"start_dt": "2025-04-14", "end_dt": "2025-04-15" }'
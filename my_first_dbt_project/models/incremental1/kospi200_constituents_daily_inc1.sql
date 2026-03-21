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
where target_index_name = '코스피 200'
{% if is_incremental() %}
  and date_kst >= (
        select max(date_kst) from {{ this }}
    ) 
{% endif %}
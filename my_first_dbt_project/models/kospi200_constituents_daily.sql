select target_index_name
    , ticker
    , name
    , date_kst
from {{ source("main", "index_constituents") }}
where target_index_name = '코스피 200'

select *
from {{ ref("price_daily_merged") }}
where high < low
    or high < close
    or high < open
select *
from {{ ref("stg_live_timing__weather_data") }}

with
    raw_timing_data_interval as (
        select *
        from {{ source("ing__live_timing", "live_timing__timing_data_interval") }}
    ),
    formatted as (
        select
            value as next_car_interval,
            catching as is_catching,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_interval
    )
select *
from formatted

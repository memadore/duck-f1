with
    raw_driver_race_info as (
        select * from {{ source("ing__live_timing", "live_timing__driver_race_info") }}
    ),
    formatted as (
        select
            driver as driver_number,
            position as track_position,
            gap as gap,
            interval as interval,
            pitstops as pitstop_count,
            catching as is_catching,
            overtakestate as overtake_count,
            isout as is_out,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_race_info
    )
select *
from formatted

with
    raw_timing_stats_speeds as (
        select *
        from {{ source("ing__live_timing", "live_timing__timing_stats_speeds") }}
    ),
    formatted as (
        select
            speedtrapkey as speed_trap_key,
            value as speed,
            position as position,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_stats_speeds
    )
select *
from formatted

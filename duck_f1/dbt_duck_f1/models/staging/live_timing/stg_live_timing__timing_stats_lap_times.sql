with
    raw_timing_stats_lap_times as (
        select * from {{ source("live_timing", "timing_stats_lap_times") }}
    ),
    formatted as (
        select
            value as lap_time,
            lap as lap,
            position as position,
            driver as driver_number,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_stats_lap_times
    )
select *
from formatted

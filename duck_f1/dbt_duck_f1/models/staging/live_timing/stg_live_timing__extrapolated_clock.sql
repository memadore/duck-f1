with
    raw_driver_race_info as (
        select * from {{ source("live_timing", "extrapolated_clock") }}
    ),
    formatted as (
        select
            utc as utc_ts,
            remaining as remaining_session_time,
            extrapolating as is_extrapolated,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_race_info
    )
select *
from formatted

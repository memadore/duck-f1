with
    raw_driver_race_info as (select * from {{ source('live_timing', 'extrapolated_clock') }}),
    formatted as (
        select
            Utc as utc_ts,
            Remaining as remaining_session_time,
            Extrapolating as is_extrapolated,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_driver_race_info
    )
select *
from formatted
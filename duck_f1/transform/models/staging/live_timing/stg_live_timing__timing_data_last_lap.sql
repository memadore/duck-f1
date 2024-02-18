with
    raw_timing_data_last_lap as (
        select * from {{ source("ing__live_timing", "live_timing__timing_data_last_lap") }}
    ),
    formatted as (
        select
            value as lap_time,
            status as lap_time_status,
            overallfastest as is_overall_fastest,
            personalfastest as is_personal_fastest,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_last_lap
    )
select *
from formatted

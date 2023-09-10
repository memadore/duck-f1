with
    raw_timing_data_speeds as (
        select * from {{ source("live_timing", "timing_data_speeds") }}
    ),
    formatted as (
        select
            speedkey as speed_key,
            value as speed_value,
            status as speed_status,
            overallfastest as is_overall_fastest,
            personalfastest as is_personal_fastest,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_speeds
    )
select *
from formatted

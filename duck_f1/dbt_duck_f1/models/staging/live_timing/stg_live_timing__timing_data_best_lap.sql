with
    raw_timing_data_best_lap as (
        select * from {{ source("live_timing", "timing_data_best_lap") }}
    ),
    formatted as (
        select
            value as lap_time,
            lap as lap_key,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_best_lap
    )
select *
from formatted

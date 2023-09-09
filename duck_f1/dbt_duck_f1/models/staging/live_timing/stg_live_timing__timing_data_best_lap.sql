with
    raw_timing_data_best_lap as (select * from {{ source('live_timing', 'timing_data_best_lap') }}),
    formatted as (
        select
            Value as lap_time,
            Lap as lap_key,
            Driver as driver,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_best_lap
    )
select *
from formatted
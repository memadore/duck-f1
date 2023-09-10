with
    raw_car_data as (select * from {{ source("live_timing", "car_data") }}),
    formatted as (
        select
            capturetimestamp as capture_ts,
            carnumber as car_number,
            channel as data_channel,
            value as data_value,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_car_data
    )
select *
from formatted

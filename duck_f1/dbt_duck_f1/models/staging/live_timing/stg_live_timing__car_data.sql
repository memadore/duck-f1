with
    raw_car_data as (select * from {{ source('live_timing', 'car_data') }}),
    formatted as (
        select
            CaptureTimestamp as capture_ts,
            CarNumber as car_number,
            Channel as data_channel,
            Value as data_value,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_car_data
    )
select *
from formatted
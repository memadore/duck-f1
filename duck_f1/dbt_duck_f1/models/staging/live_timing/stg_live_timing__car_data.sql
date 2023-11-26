with
    raw_car_data as (select * from {{ source("live_timing", "car_data") }}),
    formatted as (
        select
            capturetimestamp::timestamp as capture_ts,
            carnumber as car_number,
            enginerpm as engine_rpm,
            carspeed as car_speed,
            enginegear as engine_gear,
            throttleposition as throttle_position,
            brakeposition as brake_position,
            drsstatus as drs_status,
            _streamtimestamp as _stream_ts,
            {{ live_timing__stream_ts_to_ms("_stream_ts") }},
            {{ live_timing__metadata_raw_columns() }}
        from raw_car_data
    )
select *
from formatted

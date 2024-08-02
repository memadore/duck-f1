with
raw_car_data as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__car_data"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__car_data") }}

    {% else %}

            select
                null:integer as capturetimestamp,
                null:integer as carnumber,
                null:integer as enginerpm,
                null:integer as carspeed,
                null:integer as enginegear,
                null:integer as throttleposition,
                null:integer as brakeposition,
                null:integer as drsstatus,
                null:integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

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
        {{ live_timing__metadata() }}
    from raw_car_data
)

select *
from formatted

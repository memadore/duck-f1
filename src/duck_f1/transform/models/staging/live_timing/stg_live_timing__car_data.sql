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

car_data as (
    select
        capturetimestamp::timestamp as event_utc_ts,
        carnumber as car_number,
        enginerpm as engine_rpm,
        carspeed as car_speed,
        enginegear as engine_gear,
        least(throttleposition, 100) as throttle_position,
        least(brakeposition, 100) as brake_position,
        case
            when drsstatus = 8 then 'available'
            when drsstatus >= 10 then 'open'
            else 'unavailable'
        end as drs_status,
        drsstatus as _drs_status_id,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_car_data
),

stream_segments as (
    select
        session_id,
        car_number,
        session_ts,
        count(*) as _count
    from car_data
    group by session_id, car_number, session_ts
),

stream_corrections as (
    select
        *,
        lead(session_ts) over (
            partition by session_id, car_number
            order by session_ts
        ) as _next_value,
        if(
            _next_value is null,
            to_milliseconds(200),
            (_next_value - session_ts) / _count
        ) as _correction_step
    from stream_segments
),

computed as (
    select
        car_data.session_id,
        car_data.car_number,
        car_data.engine_rpm,
        car_data.car_speed,
        car_data.engine_gear,
        car_data.throttle_position,
        car_data.brake_position,
        car_data.drs_status,
        car_data._drs_status_id,
        car_data.event_utc_ts,
        car_data.session_ts,
        -1 + row_number() over (
            partition by car_data.session_id, car_data.car_number, car_data.session_ts
            order by car_data.event_utc_ts
        ) as _correction_factor,
        car_data.session_ts
        + date_trunc('millisecond', _correction_factor * stream_correction._correction_step)
            as _correctedsession_ts
    from car_data
    left join stream_corrections as stream_correction
        on
            car_data.session_id = stream_correction.session_id
            and car_data.car_number = stream_correction.car_number
            and car_data.session_ts = stream_correction.session_ts
),

formatted as (
    select
        session_id,
        car_number,
        engine_rpm,
        car_speed,
        engine_gear,
        throttle_position,
        brake_position,
        drs_status,
        _drs_status_id,
        event_utc_ts,
        session_ts,
        _correctedsession_ts
    from computed
)

select *
from formatted

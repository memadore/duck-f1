with
formatted as (
    select
        car_data.session_id,
        car_data.car_number,
        lap_series.lap_number,
        car_data._correctedsession_ts as session_ts,
        car_data.engine_rpm,
        car_data.car_speed,
        car_data.engine_gear,
        car_data.throttle_position,
        car_data.brake_position,
        car_data.drs_status
    from {{ ref("stg_live_timing__car_data") }} as car_data
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            car_data.session_id = lap_series.session_id
            and car_data.car_number = lap_series.car_number
            and (
                car_data._correctedsession_ts > lap_series.lap_start_ts
                and car_data._correctedsession_ts <= lap_series.lap_end_ts
            )
    where lap_series.lap_number > 0
)

select *
from formatted

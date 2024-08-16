with
formatted as (
    select
        car_position.session_id,
        car_position.car_number,
        lap_series.lap_number,
        car_position._correctedsession_ts as session_ts,
        car_position.car_status,
        car_position.x_position,
        car_position.y_position,
        car_position.z_position
    from {{ ref("stg_live_timing__position") }} as car_position
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            car_position.session_id = lap_series.session_id
            and car_position.car_number = lap_series.car_number
            and (
                car_position._correctedsession_ts > lap_series.lap_start_ts
                and car_position._correctedsession_ts <= lap_series.lap_end_ts
            )
    where lap_series.lap_number > 0
)

select *
from formatted

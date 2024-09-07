with
formatted as (
    select
        car_position.session_id,
        _session.driver_id,
        {{ dbt_utils.generate_surrogate_key(
            [
            "car_position.session_id",
            "_session.driver_id",
            "car_position.lap_number"
            ]
        ) }} as lap_id,
        car_position.lap_number,
        car_position.session_ts,
        car_position.car_status,
        car_position.x_position,
        car_position.y_position,
        car_position.z_position
    from {{ ref("int_live_timing__car__positions") }} as car_position
    left join {{ ref("mart_fct_session__drivers") }} as _session
        on
            car_position.session_id = _session.session_id
            and car_position.car_number = _session.car_number
)

select *
from formatted

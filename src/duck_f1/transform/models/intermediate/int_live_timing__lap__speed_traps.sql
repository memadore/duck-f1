with
raw_timing_data_speeds as (
    select *
    from {{ ref("stg_live_timing__timing_data_speeds") }}
),

all_other_speeds as (
    select
        speed.session_id,
        speed.car_number,
        lap_series.lap_number,
        speed.speed_key,
        speed.speed_value,
        speed.session_ts
    from raw_timing_data_speeds as speed
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            speed.session_id = lap_series.session_id
            and speed.car_number = lap_series.car_number
            and (
                speed.session_ts > lap_series.lap_start_ts
                and speed.session_ts <= lap_series.lap_end_ts
            )
    where speed.speed_key != 'FL'
),

finish_line_speed as (
    select
        speed.session_id,
        speed.car_number,
        lap_series.lap_number,
        speed.speed_key,
        speed.speed_value,
        speed.session_ts
    from raw_timing_data_speeds as speed
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            speed.session_id = lap_series.session_id
            and speed.car_number = lap_series.car_number
            and (
                speed.session_ts >= lap_series.lap_end_ts - to_milliseconds(5000)
                and speed.session_ts <= lap_series.lap_end_ts + to_milliseconds(5000)
            )
    where speed.speed_key = 'FL'
),

formatted as (
    select *
    from all_other_speeds
    union all
    select *
    from finish_line_speed
)

select *
from formatted

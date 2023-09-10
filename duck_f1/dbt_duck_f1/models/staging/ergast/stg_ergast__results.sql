with
    raw_results as (select * from {{ source("ergast", "results") }}),
    formatted as (
        select
            resultid as ergast_result_id,
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            constructorid as ergast_constructor_id,
            number as driver_number,
            grid as grid_position,
            position as position,
            positiontext as positiontext,
            positionorder::integer as position_order,
            points as points,
            laps as lap_count,
            time as race_time_label,
            milliseconds as race_time_milliseconds,
            fastestlap as fastest_lap,
            rank as rank,
            fastestlaptime as fastest_lap_time,
            fastestlapspeed as fastest_lap_speed,
            statusid as status_id
        from raw_results
    )
select *
from formatted

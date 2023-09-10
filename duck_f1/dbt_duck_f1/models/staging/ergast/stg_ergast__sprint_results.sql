with
    raw_sprint_results as (select * from {{ source("ergast", "sprint_results") }}),
    formatted as (
        select
            resultid as ergast_result_id,
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            constructorid as constructor_id,
            number as driver_number,
            grid as grid_position,
            position as position,
            positiontext as positiontext,
            positionorder as position_order,
            points as points,
            laps as lap_count,
            time as race_time_label,
            milliseconds as race_time_milliseconds,
            fastestlap as fastest_lap,
            fastestlaptime as fastest_lap_time,
            statusid as status_id
        from raw_sprint_results
    )
select *
from formatted

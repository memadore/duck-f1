with
    raw_results as (select * from {{ source('ergast', 'results') }}),
    formatted as (
        select
            resultid as result_id,
            raceid as race_id,
            driverid as driver_id,
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
            rank as rank,
            fastestlaptime as fastest_lap_time,
            fastestlapspeed as fastest_lap_speed,
            statusid as status_id
        from raw_results
    )
select *
from formatted
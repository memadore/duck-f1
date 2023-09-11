with
    raw_results as (select * from {{ source("ergast", "results") }}),
    foreign_keys as (
        select *
        from raw_results r
        join
            (
                select constructor_id, ergast_constructor_id
                from {{ ref("stg_ergast__constructors") }}
            ) construc
            on r.constructorid = construc.ergast_constructor_id
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on r.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on r.raceid = race.ergast_race_id
        join
            (
                select status_id, ergast_status_id from {{ ref("stg_ergast__status") }}
            ) status
            on r.statusid = status.ergast_status_id
    ),
    formatted as (
        select
            race_id,
            driver_id,
            constructor_id,
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
            status_id
        from foreign_keys
    )
select *
from formatted

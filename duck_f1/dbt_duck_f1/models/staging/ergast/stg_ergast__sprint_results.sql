with
    raw_sprint_results as (select * from {{ source("ergast", "sprint_results") }}),
    foreign_keys as (
        select *
        from raw_sprint_results sr
        join
            (
                select constructor_id, ergast_constructor_id
                from {{ ref("stg_ergast__constructors") }}
            ) construc
            on sr.constructorid = construc.ergast_constructor_id
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on sr.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on sr.raceid = race.ergast_race_id
        join
            (
                select status_id, ergast_status_id from {{ ref("stg_ergast__status") }}
            ) status
            on sr.statusid = status.ergast_status_id
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
            positionorder as position_order,
            points as points,
            laps as lap_count,
            time as race_time_label,
            milliseconds as race_time_milliseconds,
            fastestlap as fastest_lap,
            fastestlaptime as fastest_lap_time,
            status_id
        from foreign_keys
    )
select *
from formatted

with
    raw_lap_times as (select * from {{ source("ing__ergast", "ergast__lap_times") }}),
    foreign_keys as (
        select *
        from raw_lap_times lp
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on lp.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on lp.raceid = race.ergast_race_id
    ),
    formatted as (
        select
            driver_id,
            race_id,
            lap as lap_number,
            position as position,
            time as lap_time_label,
            milliseconds as lap_time_ms
        from foreign_keys
    )
select *
from formatted

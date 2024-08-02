with
    raw_pit_stops as (select * from {{ source("src_ergast", "ergast__pit_stops") }}),
    foreign_keys as (
        select *
        from raw_pit_stops ps
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on ps.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on ps.raceid = race.ergast_race_id
    ),
    formatted as (
        select
            driver_id,
            race_id,
            stop as stop_count,
            lap as lap_number,
            time as pit_stop_time_label,
            duration as pit_stop_duration_label,
            milliseconds as lap_time_duration_ms
        from foreign_keys
    )
select *
from formatted

with
raw_lap_times as (select * from {{ source("src_ergast", "ergast__lap_times") }}),

driver_ids as (
    select
        driver_id,
        ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

race_ids as (
    select
        race_id,
        ergast_race_id
    from {{ ref("stg_ergast__races") }}
),

formatted as (
    select
        driver.driver_id,
        race.race_id,
        lap_time.lap as lap_number,
        lap_time.position,
        lap_time.time as lap_time_label,
        lap_time.milliseconds as lap_time_ms
    from raw_lap_times as lap_time
    inner join driver_ids as driver on lap_time.driverid = driver.ergast_driver_id
    inner join race_ids as race on lap_time.raceid = race.ergast_race_id
)

select *
from formatted

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

lap_times as (
    select
        race.race_id,
        driver.driver_id,
        lap_time.lap::integer as lap_number,
        {{ dbt_utils.generate_surrogate_key(
            [
            "race.race_id",
            "driver.driver_id",
            "lap_number"
            ]
        ) }} as lap_id,
        to_milliseconds(lap_time.milliseconds) as lap_time
    from raw_lap_times as lap_time
    inner join driver_ids as driver on lap_time.driverid = driver.ergast_driver_id
    inner join race_ids as race on lap_time.raceid = race.ergast_race_id
),

lap_time_windows as (
    select
        *,
        rank() over (partition by race_id order by lap_time) as race_rank,
        rank() over (partition by race_id, lap_number order by lap_time) as race_lap_rank,
        rank() over (partition by race_id, driver_id order by lap_time) as race_driver_rank,
        sum(epoch(lap_time))
            over (partition by race_id, driver_id order by lap_number)
            as driver_race_time_seconds
    from lap_times
),

lap_time_stats as (
    select
        *,
        if(race_rank = 1, true, false) as is_race_fastest_lap,
        if(race_lap_rank = 1, true, false) as is_current_lap_fastest_lap,
        if(race_driver_rank = 1, true, false) as is_driver_race_fastest_lap,
        to_milliseconds(driver_race_time_seconds * 1000) as driver_race_time
    from lap_time_windows
),

formatted as (
    select
        lap_time.lap_id,
        lap_time.race_id,
        lap_time.driver_id,
        lap_time.lap_number,
        lap_time.lap_time,
        lap_time.is_race_fastest_lap,
        lap_time.is_current_lap_fastest_lap,
        lap_time.is_driver_race_fastest_lap,
        lap_time.driver_race_time
    from lap_time_stats as lap_time
)

select *
from formatted
order by race_id, lap_number

with
raw_sprint_results as (
    select * from {{ source("src_ergast", "ergast__sprint_results") }}
),

constructor_ids as (
    select
        constructor_id,
        ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

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

sprints as (
    select
        race.race_id,
        driver.driver_id,
        constructor.constructor_id,
        if(sprint.fastestlap = '\N', null, sprint.fastestlap::integer) as fastest_lap,
        if(
            sprint.fastestlaptime = '\N',
            null,
            {{ ergast__lap_time_to_interval("sprint.fastestlaptime") }}
        ) as fastest_lap_time
    from raw_sprint_results as sprint
    inner join
        constructor_ids as constructor
        on sprint.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on sprint.driverid = driver.ergast_driver_id
    inner join race_ids as race on sprint.raceid = race.ergast_race_id
),

sprints_windows as (
    select
        *,
        -- TODO: Get rank from lap timestamp. Current rank won't be accurate for equal lap times.
        rank()
            over (partition by race_id order by fastest_lap_time)
            as fastest_lap_rank,
        fastest_lap_time
        - first(fastest_lap_time)
            over (partition by race_id order by fastest_lap_time)
            as fastest_lap_time_interval,
        fastest_lap_time
        - lag(fastest_lap_time)
            over (partition by race_id order by fastest_lap_time)
            as fastest_lap_time_gap
    from sprints
),

formatted as (
    select
        sprint.race_id,
        sprint.driver_id,
        sprint.constructor_id,
        sprint.fastest_lap,
        sprint.fastest_lap_rank,
        sprint.fastest_lap_time,
        sprint.fastest_lap_time_gap,
        if(
            sprint.fastest_lap_time_interval = to_milliseconds(0),
            null,
            sprint.fastest_lap_time_interval
        ) as fastest_lap_time_interval
    from sprints_windows as sprint
)

select *
from formatted

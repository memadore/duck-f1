with
raw_results as (select * from {{ source("src_ergast", "ergast__results") }}),

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

results as (
    select
        race.race_id,
        driver.driver_id,
        constructor.constructor_id,
        if(result.fastestlap = '\N', null, result.fastestlap::integer) as fastest_lap,
        if(result.rank = '\N', null, result.rank::integer) as fastest_lap_rank,
        if(
            result.fastestlaptime = '\N',
            null,
            {{ ergast__lap_time_to_interval("result.fastestlaptime") }}
        ) as fastest_lap_time,
        if(result.fastestlapspeed = '\N', null, result.fastestlapspeed::numeric)
            as fastest_lap_speed
    from raw_results as result
    inner join
        constructor_ids as constructor
        on result.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on result.driverid = driver.ergast_driver_id
    inner join race_ids as race on result.raceid = race.ergast_race_id
    where fastest_lap_rank > 0
),

results_stats as (
    select
        *,
        fastest_lap_time
        - first(fastest_lap_time)
            over (partition by race_id order by fastest_lap_rank)
            as fastest_lap_time_interval,
        fastest_lap_time
        - lag(fastest_lap_time)
            over (partition by race_id order by fastest_lap_rank)
            as fastest_lap_time_gap
    from results
),

formatted as (
    select
        result.race_id,
        result.driver_id,
        result.constructor_id,
        result.fastest_lap,
        result.fastest_lap_rank,
        result.fastest_lap_time,
        result.fastest_lap_time_interval,
        result.fastest_lap_time_gap,
        result.fastest_lap_speed
    from results_stats as result
)

select *
from formatted

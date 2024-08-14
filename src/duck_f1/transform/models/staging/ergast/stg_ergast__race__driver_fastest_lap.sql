with
raw_results as (select * from {{ source("src_ergast", "ergast__results") }}),

constructor_ids as (
    select
        constructor_id,
        _ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

driver_ids as (
    select
        driver_id,
        _ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

session_ids as (
    select
        session_id,
        _ergast_race_id
    from {{ ref("stg_sessions") }}
    where session_type = 'race'
),

results as (
    select
        _session.session_id,
        driver.driver_id,
        constructor.constructor_id,
        if(result.fastestlap = '\N', null, result.fastestlap::integer) as fastest_lap,
        if(result.rank = '\N', null, result.rank::integer) as fastest_lap_rank,
        if(
            result.fastestlaptime = '\N',
            null,
            {{ varchar_lap_time_to_interval("result.fastestlaptime") }}
        ) as fastest_lap_time,
        if(result.fastestlapspeed = '\N', null, result.fastestlapspeed::numeric)
            as fastest_lap_speed
    from raw_results as result
    inner join
        constructor_ids as constructor
        on result.constructorid = constructor._ergast_constructor_id
    inner join driver_ids as driver on result.driverid = driver._ergast_driver_id
    inner join session_ids as _session on result.raceid = _session._ergast_race_id
    where fastest_lap_rank > 0
),

results_stats as (
    select
        *,
        fastest_lap_time
        - first(fastest_lap_time)
            over (partition by session_id order by fastest_lap_rank)
            as fastest_lap_time_interval,
        fastest_lap_time
        - lag(fastest_lap_time)
            over (partition by session_id order by fastest_lap_rank)
            as fastest_lap_time_gap
    from results
),

formatted as (
    select
        result.session_id,
        result.driver_id,
        result.constructor_id,
        result.fastest_lap,
        result.fastest_lap_rank,
        result.fastest_lap_time,
        result.fastest_lap_time_gap,
        result.fastest_lap_speed,
        if(
            result.fastest_lap_time_interval = to_milliseconds(0),
            null,
            result.fastest_lap_time_interval
        ) as fastest_lap_time_interval
    from results_stats as result
)

select *
from formatted

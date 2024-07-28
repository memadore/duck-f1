with
raw_results as (select * from {{ source("ing__ergast", "ergast__results") }}),

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

status_ids as (
    select
        status_id,
        ergast_status_id
    from {{ ref("stg_ergast__status") }}
),

formatted as (
    select
        race.race_id,
        driver.driver_id,
        constructor.constructor_id,
        result.number as driver_number,
        result.position,
        result.positiontext,
        result.positionorder::integer as position_order,
        result.points,
        result.laps as lap_count,
        result.time as race_time_label,
        result.milliseconds as race_time_milliseconds,
        result.fastestlap as fastest_lap,
        result.rank,
        result.fastestlaptime as fastest_lap_time,
        result.fastestlapspeed as fastest_lap_speed,
        driver_status.status_id,
        case when result.grid > 0 then result.grid end as grid_position
    from raw_results as result
    inner join
        constructor_ids as constructor
        on result.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on result.driverid = driver.ergast_driver_id
    inner join race_ids as race on result.raceid = race.ergast_race_id
    inner join status_ids as driver_status on result.statusid = driver_status.ergast_status_id
)

select *
from formatted

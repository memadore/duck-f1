with
raw_sprint_results as (
    select * from {{ source("ing__ergast", "ergast__sprint_results") }}
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
        sprint.number as driver_number,
        sprint.grid as grid_position,
        sprint.positiontext,
        sprint.positionorder as position_order,
        sprint.points,
        sprint.laps as lap_count,
        sprint.time as race_time_label,
        sprint.milliseconds as race_time_milliseconds,
        sprint.fastestlap as fastest_lap,
        sprint.fastestlaptime as fastest_lap_time,
        driver_status.status_id
    from raw_sprint_results as sprint
    inner join
        constructor_ids as constructor
        on sprint.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on sprint.driverid = driver.ergast_driver_id
    inner join race_ids as race on sprint.raceid = race.ergast_race_id
    inner join status_ids as driver_status on sprint.statusid = driver_status.ergast_status_id
)

select *
from formatted

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

status_ids as (
    select
        status_id,
        ergast_status_id
    from {{ ref("stg_ergast__status") }}
),

sprints as (
    select
        race.race_id,
        driver.driver_id,
        constructor.constructor_id,
        sprint.positiontext as position_label,
        sprint.positionorder::integer as position_order,
        driver_status.status_id,
        sprint.points,
        sprint.laps as laps_completed,
        sprint.number::integer as driver_number,
        if(sprint.grid > 0, sprint.grid, null) as grid_position,
        if(sprint.position = '\N', null, sprint.position::integer) as classification,
        if(sprint.milliseconds = '\N', null, to_milliseconds(sprint.milliseconds::integer))
            as sprint_time,
        if(sprint.time = '\N', null, sprint.time) as sprint_time_label
    from raw_sprint_results as sprint
    inner join
        constructor_ids as constructor
        on sprint.constructorid = constructor.ergast_constructor_id
    inner join driver_ids as driver on sprint.driverid = driver.ergast_driver_id
    inner join race_ids as race on sprint.raceid = race.ergast_race_id
    inner join status_ids as driver_status on sprint.statusid = driver_status.ergast_status_id
),

sprints_windows as (
    select
        *,
        sprint_time
        - first(sprint_time)
            over (partition by race_id order by position_order)
            as sprint_time_interval,
        sprint_time
        - lag(sprint_time) over (partition by race_id order by position_order) as sprint_time_gap
    from sprints
),

formatted as (
    select
        sprint.race_id,
        sprint.driver_id,
        sprint.constructor_id,
        sprint.driver_number,
        sprint.grid_position,
        sprint.classification,
        sprint.position_label,
        sprint.position_order,
        sprint.status_id,
        sprint.points,
        sprint.laps_completed,
        sprint.sprint_time,
        sprint.sprint_time_label,
        sprint.sprint_time_interval,
        sprint.sprint_time_gap
    from sprints_windows as sprint
)


select *
from formatted

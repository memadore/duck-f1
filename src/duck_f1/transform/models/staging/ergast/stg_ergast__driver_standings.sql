with
raw_driver_standings as (
    select * from {{ source("src_ergast", "ergast__driver_standings") }}
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

formatted as (
    select
        driver.driver_id,
        race.race_id,
        driver_standing.points,
        driver_standing.position,
        driver_standing.positiontext as position_label,
        driver_standing.wins as win_count
    from raw_driver_standings as driver_standing
    inner join driver_ids as driver on driver_standing.driverid = driver.ergast_driver_id
    inner join race_ids as race on driver_standing.raceid = race.ergast_race_id
)

select *
from formatted

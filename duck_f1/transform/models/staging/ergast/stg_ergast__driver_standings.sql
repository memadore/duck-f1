with
    raw_driver_standings as (
        select * from {{ source("ing__ergast", "ergast__driver_standings") }}
    ),
    foreign_keys as (
        select *
        from raw_driver_standings ds
        join
            (
                select driver_id, ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on ds.driverid = driver.ergast_driver_id
        join
            (select race_id, ergast_race_id from {{ ref("stg_ergast__races") }}) race
            on ds.raceid = race.ergast_race_id
    ),
    formatted as (
        select
            driver_id,
            race_id,
            points as points,
            position as position,
            positiontext as position_label,
            wins as win_count
        from foreign_keys
    )
select *
from formatted

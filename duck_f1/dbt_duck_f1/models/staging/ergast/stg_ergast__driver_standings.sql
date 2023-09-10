with
    raw_driver_standings as (select * from {{ source("ergast", "driver_standings") }}),
    formatted as (
        select
            driverstandingsid as ergast_driver_standings_id,
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            points as points,
            position as position,
            positiontext as position_label,
            wins as win_count
        from raw_driver_standings
    )
select *
from formatted

with
    raw_driver_standings as (select * from {{ source('ergast', 'driver_standings') }}),
    formatted as (
        select
            driverstandingsid as driver_standings_id,
            raceid as race_id,
            driverid as driver_id,
            points as points,
            position as position,
            positiontext as position_label,
            wins as win_count
        from raw_driver_standings
    )
select *
from formatted
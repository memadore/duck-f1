with
    raw_qualifying as (select * from {{ source("ergast", "qualifying") }}),
    formatted as (
        select
            qualifyid as ergast_qualifying_id,
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            constructorid as ergast_constructor_id,
            number as driver_number,
            position as position,
            q1 as q1_time_label,
            q2 as q2_time_label,
            q3 as q3_time_label
        from raw_qualifying
    )
select *
from formatted

with
    raw_lap_times as (select * from {{ source("ergast", "lap_times") }}),
    formatted as (
        select
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            lap as lap_number,
            position as position,
            time as lap_time_label,
            milliseconds as lap_time_ms
        from raw_lap_times
    )
select *
from formatted

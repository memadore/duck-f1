with
    raw_pit_stops as (select * from {{ source("ergast", "pit_stops") }}),
    formatted as (
        select
            raceid as ergast_race_id,
            driverid as ergast_driver_id,
            stop as stop_count,
            lap as lap_number,
            time as pit_stop_time_label,
            duration as pit_stop_duration_label,
            milliseconds as lap_time_duration_ms
        from raw_pit_stops
    )
select *
from formatted

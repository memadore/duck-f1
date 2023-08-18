with
    raw_pit_stops as (select * from {{ source('ergast', 'pit_stops') }}),
    formatted as (
        select
            raceid as race_id,
            driverid as driver_id,
            stop as stop_count,
            lap as lap_id,
            time as pit_stop_time_label,
            duration as pit_stop_duration_label,
            milliseconds as lap_time_duration_ms
        from raw_pit_stops
    )
select *
from formatted
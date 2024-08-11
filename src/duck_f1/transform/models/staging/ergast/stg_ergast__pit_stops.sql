with
    raw_pit_stops as (select * from {{ source("src_ergast", "ergast__pit_stops") }}),
    foreign_keys as (
        select *
        from raw_pit_stops ps
        join
            (
                select driver_id, _ergast_driver_id from {{ ref("stg_ergast__drivers") }}
            ) driver
            on ps.driverid = driver._ergast_driver_id
        join
            (select session_id, _ergast_race_id from {{ ref("stg_ergast__sessions") }}) race
            on ps.raceid = _session._ergast_race_id
    ),
    formatted as (
        select
            driver_id,
            session_id,
            stop as stop_count,
            lap as lap_number,
            time as pit_stop_time_label,
            duration as pit_stop_duration_label,
            milliseconds as lap_time_duration_ms
        from foreign_keys
    )
select *
from formatted

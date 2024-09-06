with
raw_pit_stops as (select * from {{ source("src_ergast", "ergast__pit_stops") }}),

driver_ids as (
    select
        driver_id,
        _ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

session_ids as (
    select
        session_id,
        _ergast_race_id
    from {{ ref("stg_sessions") }}
    where session_type = 'race'
),

formatted as (
    select
        driver.driver_id,
        _session.session_id,
        pit_stop.stop::integer as stop_count,
        pit_stop.lap::integer as lap_number,
        pit_stop.duration as pit_stop_time_label,
        to_milliseconds(pit_stop.milliseconds) as pit_stop_time
    from raw_pit_stops as pit_stop
    inner join driver_ids as driver on pit_stop.driverid = driver._ergast_driver_id
    inner join session_ids as _session on pit_stop.raceid = _session._ergast_race_id
)

select *
from formatted

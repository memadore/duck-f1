with
raw_driver_standings as (
    select * from {{ source("src_ergast", "ergast__driver_standings") }}
),

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
        driver_standing.points,
        driver_standing.position,
        driver_standing.positiontext as position_label,
        driver_standing.wins as win_count
    from raw_driver_standings as driver_standing
    inner join driver_ids as driver on driver_standing.driverid = driver._ergast_driver_id
    inner join session_ids as _session on driver_standing.raceid = _session._ergast_race_id
)

select *
from formatted

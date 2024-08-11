with
raw_lap_times as (select * from {{ source("src_ergast", "ergast__lap_times") }}),

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

driver_classifications as (
    select
        session_id,
        driver_id,
        grid_position
    from {{ ref("stg_ergast__race__driver_classification") }}
),

lap_positions as (
    select
        _session.session_id,
        driver.driver_id,
        lap_time.lap::integer as lap_number,
        {{ dbt_utils.generate_surrogate_key(
            [
            "_session.session_id",
            "driver.driver_id",
            "lap_number"
            ]
        ) }} as lap_id,
        driver_classification.grid_position as start_position,
        lap_time.position::integer as driver_position,
        (start_position - driver_position) * (-1) as positions_gained_from_start
    from raw_lap_times as lap_time
    inner join driver_ids as driver on lap_time.driverid = driver._ergast_driver_id
    inner join session_ids as _session on lap_time.raceid = _session._ergast_race_id
    left join driver_classifications as driver_classification
        on
            _session.session_id = driver_classification.session_id
            and driver.driver_id = driver_classification.driver_id
),

lap_position_windows as (
    select
        *,
        -driver_position + lag(driver_position)
            over (partition by session_id, driver_id order by lap_number)
            as positions_gained_this_lap
    from lap_positions
),

formatted as (
    select
        lap_position.lap_id,
        lap_position.session_id,
        lap_position.driver_id,
        lap_position.lap_number,
        lap_position.driver_position,
        -- lap_position.start_position,
        lap_position.positions_gained_from_start,
        coalesce(
            lap_position.positions_gained_this_lap,
            lap_position.positions_gained_from_start
        ) as positions_gained_this_lap
    from lap_position_windows as lap_position
)

select *
from formatted
order by session_id, lap_number

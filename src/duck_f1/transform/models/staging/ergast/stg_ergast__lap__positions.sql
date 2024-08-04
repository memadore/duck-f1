with
raw_lap_times as (select * from {{ source("src_ergast", "ergast__lap_times") }}),

driver_ids as (
    select
        driver_id,
        ergast_driver_id
    from {{ ref("stg_ergast__drivers") }}
),

race_ids as (
    select
        race_id,
        ergast_race_id
    from {{ ref("stg_ergast__races") }}
),

driver_classifications as (
    select
        race_id,
        driver_id,
        grid_position
    from {{ ref("stg_ergast__race__drivers_classification") }}
),

lap_positions as (
    select
        race.race_id,
        driver.driver_id,
        lap_time.lap::integer as lap_number,
        {{ dbt_utils.generate_surrogate_key(
            [
            "race.race_id",
            "driver.driver_id",
            "lap_number"
            ]
        ) }} as lap_id,
        driver_classification.grid_position as start_position,
        lap_time.position::integer as driver_position,
        (start_position - driver_position) * (-1) as positions_gained_from_start
    from raw_lap_times as lap_time
    inner join driver_ids as driver on lap_time.driverid = driver.ergast_driver_id
    inner join race_ids as race on lap_time.raceid = race.ergast_race_id
    left join driver_classifications as driver_classification
        on
            race.race_id = driver_classification.race_id
            and driver.driver_id = driver_classification.driver_id
),

lap_position_windows as (
    select
        *,
        -driver_position + lag(driver_position)
            over (partition by race_id, driver_id order by lap_number)
            as positions_gained_this_lap
    from lap_positions
),

formatted as (
    select
        lap_position.lap_id,
        lap_position.race_id,
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
order by race_id, lap_number

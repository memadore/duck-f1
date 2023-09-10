with
    round_total as (
        select
            driver.driver_id,
            year,
            standing.race_id,
            win_count,
            result.position_order as finish_position,
            sum(standing.points) over season as season_total,
            min(result.position_order) over cummulative_season as best_position,
            avg(result.position_order) over cummulative_season as avg_position,
        from {{ ref("stg_ergast__driver_standings") }} standing
        join
            {{ ref("stg_ergast__drivers") }} driver
            on driver.ergast_driver_id = standing.ergast_driver_id
        join
            {{ ref("stg_ergast__races") }} race
            on race.ergast_race_id = standing.ergast_race_id
        join
            {{ ref("stg_ergast__results") }} result
            on (
                result.ergast_race_id = standing.ergast_race_id
                and result.ergast_driver_id = standing.ergast_driver_id
            )
        window
            season as (partition by driver.driver_id, year, standing.race_id),
            cummulative_season as (
                partition by year, driver.driver_id order by standing.race_id
            )
        order by year, standing.race_id, driver.driver_id
    ),
    classification as (
        select
            *,
            rank() over (
                partition by year, race_id
                order by season_total desc, best_position, avg_position
            ) as championship_position,
            season_total - lag(season_total, 1, 0) over (
                partition by year, driver_id order by race_id
            ) as round_points
        from round_total
    )
select
    *,
    - championship_position
    + lag(championship_position, 1, championship_position) over (
        partition by year, driver_id order by race_id
    ) as positions_gained
from classification
where year = 2023
order by year, race_id, season_total desc

with
    round_stats as (
        select
            result.driver_id,
            result.race_id,
            race.year as race_year,
            race.round as race_round,
            grid_position,
            min(grid_position) over cummulative_season as best_grid_position,
            max(grid_position) over cummulative_season as worst_grid_position,
            avg(grid_position) over cummulative_season as avg_grid_position,
            result.position_order as finish_position,
            min(result.position_order) over cummulative_season as best_finish_position,
            max(result.position_order) over cummulative_season as worst_finish_position,
            avg(result.position_order) over cummulative_season as avg_finish_position,
            (grid_position - finish_position) as position_gained,
            sum(position_gained) over cummulative_season as total_position_gained,
        from {{ ref("stg_ergast__results") }} result
        join {{ ref("stg_ergast__races") }} race on race.race_id = result.race_id
        window
            cummulative_season as (
                partition by year, result.driver_id order by race_year, race_round
            )
    )
select *
from round_stats
order by race_year, race_round, finish_position

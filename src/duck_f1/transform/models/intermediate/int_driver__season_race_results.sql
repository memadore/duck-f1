with
round_stats as (
    select
        result.driver_id,
        result.session_id,
        _session.year as race_year,
        _session.round as race_round,
        result.grid_position,
        result.position_order as finish_position,
        min(result.grid_position) over cummulative_season as best_grid_position,
        max(result.grid_position) over cummulative_season as worst_grid_position,
        avg(result.grid_position) over cummulative_season as avg_grid_position,
        min(result.position_order) over cummulative_season as best_finish_position,
        max(result.position_order) over cummulative_season as worst_finish_position,
        avg(result.position_order) over cummulative_season as avg_finish_position,
        (result.grid_position - finish_position) as position_gained,
        sum(position_gained) over cummulative_season as total_position_gained
    from {{ ref("stg_ergast__race__driver_classification") }} as result
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on result.session_id = _session.session_id
    window
        cummulative_season as (
            partition by year, result.driver_id order by race_year, race_round
        )
)

select *
from round_stats

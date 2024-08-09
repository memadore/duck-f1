with
round_stats as (
    select
        driver.driver_id,
        standing.session_id,
        _session.year as season,
        _session.round as race_round,
        standing.position as championship_position,
        standing.win_count,
        standing.points as season_total,
        season_total
        - lag(season_total, 1, 0) over cummulative_season as round_points,
        -championship_position
        + lag(
            championship_position, 1, championship_position
        ) over cummulative_season as positions_gained
    from {{ ref("stg_ergast__standings__drivers") }} as standing
    inner join
        {{ ref("stg_ergast__drivers") }} as driver
        on standing.driver_id = driver.driver_id
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on standing.session_id = _session.session_id
    window
        cummulative_season as (
            partition by season, standing.driver_id order by season, race_round
        )
)

select *
from round_stats

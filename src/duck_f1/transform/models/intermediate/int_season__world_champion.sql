with
round_stats as (
    select
        driver.driver_id,
        standing.session_id,
        _session.year as season,
        _session.race_time_utc,
        _session.round as race_round,
        standing.position as championship_position,
        standing.points as season_total,
        max(_session.round) over cummulative_season as last_round
    from {{ ref("stg_ergast__standings__drivers") }} as standing
    inner join
        {{ ref("stg_ergast__drivers") }} as driver
        on standing.driver_id = driver.driver_id
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on standing.session_id = _session.session_id
    window cummulative_season as (partition by season)
)

select
    season,
    driver_id
from round_stats
where race_round = last_round and championship_position = 1

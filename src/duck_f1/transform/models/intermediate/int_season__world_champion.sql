with
round_stats as (
    select
        driver.driver_id,
        standing.race_id,
        race.year as season,
        race.race_time_utc,
        race.round as race_round,
        standing.position as championship_position,
        standing.points as season_total,
        max(race.round) over cummulative_season as last_round
    from {{ ref("stg_ergast__driver_standings") }} as standing
    inner join
        {{ ref("stg_ergast__drivers") }} as driver
        on standing.driver_id = driver.driver_id
    inner join
        {{ ref("stg_ergast__races") }} as race
        on standing.race_id = race.race_id
    window cummulative_season as (partition by season)
)

select
    season,
    driver_id
from round_stats
where race_round = last_round and championship_position = 1

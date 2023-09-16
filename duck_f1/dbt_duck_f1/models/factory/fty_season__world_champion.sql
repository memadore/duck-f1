with
    round_stats as (
        select
            driver.driver_id,
            standing.race_id,
            race.year as season,
            race_time_gmt,
            race.round as race_round,
            max(race.round) over cummulative_season as last_round,
            standing.position as championship_position,
            standing.points as season_total
        from {{ ref("stg_ergast__driver_standings") }} standing
        join
            {{ ref("stg_ergast__drivers") }} driver
            on driver.driver_id = standing.driver_id
        right join
            {{ ref("stg_ergast__races") }} race on race.race_id = standing.race_id
        window cummulative_season as (partition by season)
    )
select season, driver_id
from round_stats
where race_round = last_round and championship_position = 1
order by season

with
results as (
    select
        result.driver_id,
        round(avg(result.grid_position)::numeric, 2)::text as avg_grid_position,
        round(avg(result.position_order)::numeric, 2)::text as avg_finish_position,
        round(avg(result.points)::numeric, 2)::text as avg_points,
        count(*) as event_count,
        min(
            case when result.grid_position > 0 then result.grid_position end
        ) as top_grid_position,
        count(case when result.position_order = 1 then 1 end) as win_count,
        (win_count / event_count) as win_ratio,
        max(case when result.position_order = 1 then _session.date end) as latest_win_date,
        count(case when result.position_order <= 3 then 1 end) as podium_count,
        (podium_count / event_count) as podium_ratio,
        max(case when result.position_order <= 3 then _session.date end) as latest_podium_date,
        min(result.position_order) as top_finish_position,
        max(result.position_order) as low_finish_position,
        max(result.points) as max_points,
        min(result.points) as min_points,
        sum(result.points) as total_points
    from {{ ref("stg_ergast__race__driver_classification") }} as result
    inner join
        {{ ref("stg_ergast__races") }} as _session
        on result.session_id = _session.session_id
    group by result.driver_id
),

world_championships as (
    select
        driver_id,
        count(*) as wc_count
    from {{ ref("int_season__world_champion") }}
    group by driver_id
)

select
    driver.driver_id,
    driver.driver_code,
    driver.first_name,
    driver.last_name,
    driver.full_name,
    result.event_count,
    result.avg_grid_position,
    result.win_count,
    result.latest_win_date,
    result.podium_count,
    result.avg_finish_position,
    result.top_finish_position,
    result.low_finish_position,
    result.avg_points,
    result.max_points,
    result.min_points,
    result.total_points,
    world_championship.wc_count,
    round(result.win_ratio, 2) as win_ratio,
    round(result.podium_ratio, 2) as podium_ratio
from {{ ref("stg_ergast__drivers") }} as driver
left join results as result on driver.driver_id = result.driver_id
left join
    world_championships as world_championship
    on driver.driver_id = world_championship.driver_id
order by result.win_count desc

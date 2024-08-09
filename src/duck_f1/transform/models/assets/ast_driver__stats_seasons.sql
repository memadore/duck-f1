with
results as (
    select
        result.driver_id,
        _session.year as season,
        round(avg(result.grid_position)::numeric, 2)::text as avg_grid_position,
        round(avg(result.position_order)::numeric, 2)::text as avg_finish_position,
        round(avg(result.points)::numeric, 2)::text as avg_points,
        min(
            case when result.grid_position > 0 then result.grid_position end
        ) as top_grid_position,
        count(case when result.position_order = 1 then 1 end) as win_count,
        max(case when result.position_order = 1 then _session.date end) as latest_win_date,
        count(case when result.position_order <= 3 then 1 end) as podium_count,
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
    group by result.driver_id, _session.year
)

select
    driver.driver_id,
    driver.driver_code,
    driver.last_name,
    driver.first_name,
    driver.full_name,
    result.season,
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
    result.total_points
from {{ ref("stg_ergast__drivers") }} as driver
inner join results as result on driver.driver_id = result.driver_id
order by driver.last_name, driver.first_name, result.season

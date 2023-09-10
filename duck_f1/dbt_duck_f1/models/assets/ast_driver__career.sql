with
    results as (
        select
            driver_id as driver_id,
            round(avg(grid_position)::numeric, 2)::text as avg_grid_position,
            min(
                case when grid_position > 0 then grid_position end
            ) as top_grid_position,
            count(case when position_order = 1 then 1 end) as win_count,
            max(case when position_order = 1 then race.date end) as latest_win_date,
            count(case when position_order <= 3 then 1 end) as podium_count,
            max(case when position_order <= 3 then race.date end) as latest_podium_date,
            round(avg(position_order)::numeric, 2)::text as avg_finish_position,
            min(position_order) as top_finish_position,
            max(position_order) as low_finish_position,
            round(avg(points)::numeric, 2)::text as avg_points,
            max(points) as max_points,
            min(points) as min_points,
            sum(points) as total_points
        from {{ ref("stg_ergast__results") }} as result
        join {{ ref("stg_ergast__races") }} as race on race.race_id = result.race_id
        group by driver_id
    )
select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__drivers"),
            except=["ergast_driver_id", "driver_reference"],
            relation_alias="d",
        )
    }},
    r.avg_grid_position,
    r.win_count,
    r.latest_win_date,
    r.podium_count,
    r.avg_finish_position,
    r.top_finish_position,
    r.low_finish_position,
    r.avg_points,
    r.max_points,
    r.min_points,
    r.total_points
from {{ ref("stg_ergast__drivers") }} d
inner join results r on d.ergast_driver_id = r.driver_id
order by win_count desc

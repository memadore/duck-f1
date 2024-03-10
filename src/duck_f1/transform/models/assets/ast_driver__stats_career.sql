with
    results as (
        select
            driver_id as driver_id,
            count(*) as event_count,
            round(avg(grid_position)::numeric, 2)::text as avg_grid_position,
            min(
                case when grid_position > 0 then grid_position end
            ) as top_grid_position,
            count(case when position_order = 1 then 1 end) as win_count,
            (win_count / event_count) as win_ratio,
            max(case when position_order = 1 then race.date end) as latest_win_date,
            count(case when position_order <= 3 then 1 end) as podium_count,
            (podium_count / event_count) as podium_ratio,
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
    ),
    world_championships as (
        select driver_id, count(*) as wc_count
        from {{ ref("fty_season__world_champion") }}
        group by driver_id
    )
select
    d.driver_id,
    d.driver_code,
    d.first_name,
    d.last_name,
    d.full_name,
    r.event_count,
    r.avg_grid_position,
    r.win_count,
    round(r.win_ratio, 2) as win_ratio,
    r.latest_win_date,
    r.podium_count,
    round(r.podium_ratio, 2) as podium_ratio,
    r.avg_finish_position,
    r.top_finish_position,
    r.low_finish_position,
    r.avg_points,
    r.max_points,
    r.min_points,
    r.total_points,
    wc.wc_count
from {{ ref("stg_ergast__drivers") }} d
join results r on d.driver_id = r.driver_id
join world_championships wc on d.driver_id = wc.driver_id
order by win_count desc

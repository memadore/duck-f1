with driver_stints as (
    select
        tyre_stint.session_id,
        tyre_stint.car_number,
        tyre_stint.stint_sequence,
        min(tyre_stint.session_ts) as stint_start_ts,
        min(lap_series.lap_number) as stint_start_lap_number,
        max(tyre_stint.session_ts) as stint_end_ts,
        max(lap_series.lap_number) as stint_end_lap_number,
        -- noqa: disable=all
        any_value(tyre_stint.tyre_compound) filter (
            tyre_stint.tyre_compound is not null
            and tyre_stint.tyre_compound not like 'UNKNOWN'
        ) as tyre_compound,
        -- noqa: enable=all
        min(tyre_stint.start_laps) as tyre_age_start,
        max(tyre_stint.total_laps) as tyre_age_end
    from {{ ref("stg_live_timing__tyre_stint_series") }} as tyre_stint
    left join {{ ref("stg_live_timing__lap_series") }} as lap_series
        on
            tyre_stint.session_id = lap_series.session_id
            and tyre_stint.car_number = lap_series.car_number
            and (
                tyre_stint.session_ts > lap_series.lap_start_ts
                and tyre_stint.session_ts <= lap_series.lap_end_ts
            )
    where lap_series.lap_number > 0
    group by tyre_stint.session_id, tyre_stint.car_number, tyre_stint.stint_sequence
),

computed as (
    select
        *,
        if(tyre_age_start = 0, true, false) as is_new,
        tyre_age_end - tyre_age_start as lap_count
    from driver_stints
),

formatted as (
    select
        session_id,
        car_number,
        stint_sequence,
        stint_start_ts,
        stint_start_lap_number,
        tyre_age_start,
        tyre_compound,
        is_new,
        stint_end_ts,
        stint_end_lap_number,
        tyre_age_end,
        lap_count
    from computed
    where tyre_compound is not null
)

select *
from formatted

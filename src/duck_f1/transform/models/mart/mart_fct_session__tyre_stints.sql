select
    tyre_stint.session_id,
    _session.driver_id,
    {{ dbt_utils.generate_surrogate_key(
        [
        "tyre_stint.session_id",
        "_session.driver_id",
        "tyre_stint.stint_sequence"
        ]
    ) }} as stint_id,
    tyre_stint.stint_sequence,
    tyre_stint.stint_start_ts,
    tyre_stint.stint_start_lap_number,
    tyre_stint.tyre_age_start,
    tyre_stint.tyre_compound,
    tyre_stint.is_new,
    tyre_stint.stint_end_ts,
    tyre_stint.stint_end_lap_number,
    tyre_stint.tyre_age_end,
    tyre_stint.lap_count
from {{ ref("int_live_timing__tyre_stints") }} as tyre_stint
left join {{ ref("mart_fct_session__drivers") }} as _session
    on
        tyre_stint.session_id = _session.session_id
        and tyre_stint.car_number = _session.car_number

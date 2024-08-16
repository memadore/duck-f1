with
raw_driver_race_info as (
    {% if check_if_source_exists(
        "src_live_timing", "live_timing__driver_race_info"
    ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__driver_race_info") }}

    {% else %}

        select
            null::integer as driver,
            null::integer as position,
            null::integer as gap,
            null::integer as interval,
            null::integer as pitstops,
            null::integer as catching,
            null::integer as overtakestate,
            null::integer as isout,
            null::integer as _streamtimestamp,
            {{ live_timing__empty_metadata() }}
        where false

    {% endif %}
),

formatted as (
    select
        race_info.driver as car_number,
        race_info.position as track_position,
        race_info.gap as gap_to_leader,
        race_info.interval,
        race_info.pitstops as pitstop_count,
        race_info.catching as is_catching,
        race_info.overtakestate as overtake_count,
        race_info.isout as is_out,
        race_info._streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_driver_race_info as race_info
)

select *
from formatted

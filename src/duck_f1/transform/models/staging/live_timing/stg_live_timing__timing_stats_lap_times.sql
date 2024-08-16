with
raw_timing_stats_lap_times as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_stats_lap_times"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_stats_lap_times") }}

    {% else %}

            select
                null::integer as value,
                null::integer as lap,
                null::integer as position,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

computed as (
    select
        {{ varchar_lap_time_to_interval("value") }} as lap_time,
        lap as lap_number,
        position,
        driver as car_number,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_stats_lap_times
    where len(value) > 0
),

formatted as (
    select
        session_id,
        car_number,
        lap_number,
        lap_time,
        position,
        session_ts
    from computed
)

select *
from formatted

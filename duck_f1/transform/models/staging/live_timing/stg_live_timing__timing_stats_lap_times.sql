with
    raw_timing_stats_lap_times as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__timing_stats_lap_times"
        ) | trim == "True" %}

            select *
            from {{ source("ing__live_timing", "live_timing__timing_stats_lap_times") }}

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
    formatted as (
        select
            value as lap_time,
            lap as lap,
            position as position,
            driver as driver_number,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_timing_stats_lap_times
    )
select *
from formatted

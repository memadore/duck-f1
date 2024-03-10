with
    raw_timing_stats_speeds as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__timing_stats_speeds"
        ) | trim == "True" %}

            select *
            from {{ source("ing__live_timing", "live_timing__timing_stats_speeds") }}

        {% else %}

            select
                null::integer as speedtrapkey,
                null::integer as value,
                null::integer as position,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
    ),
    formatted as (
        select
            speedtrapkey as speed_trap_key,
            value as speed,
            position as position,
            driver as driver,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_timing_stats_speeds
    )
select *
from formatted

with
    raw_driver_race_info as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__driver_race_info"
        ) | trim == "True" %}

            select *
            from {{ source("ing__live_timing", "live_timing__driver_race_info") }}

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
            driver as driver_number,
            position as track_position,
            gap as gap,
            interval as interval,
            pitstops as pitstop_count,
            catching as is_catching,
            overtakestate as overtake_count,
            isout as is_out,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata() }}
        from raw_driver_race_info
    )
select *
from formatted

with
raw_pit_lane_time_collection as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__pit_lane_time_collection"
        ) | trim == "True" %}

        select *
        from
                {{
                    source(
                        "src_live_timing", "live_timing__pit_lane_time_collection"
                    )
                }}

    {% else %}

            select
                null::integer as driver,
                null::integer as duration,
                null::integer as lap,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        driver,
        duration as pit_lane_time_duration,
        lap as lap_number,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_pit_lane_time_collection
)

select *
from formatted

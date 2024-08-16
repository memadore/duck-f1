with
raw_timing_data_best_lap as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_best_lap"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_best_lap") }}

    {% else %}

            select
                null::integer as value,
                null::integer as lap,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        value as lap_time,
        lap as lap_key,
        driver,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_best_lap
)

select *
from formatted

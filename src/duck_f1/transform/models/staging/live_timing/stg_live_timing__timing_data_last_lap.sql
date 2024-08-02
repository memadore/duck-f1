with
raw_timing_data_last_lap as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_last_lap"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_last_lap") }}

    {% else %}

            select
                null::integer as value,
                null::integer as status,
                null::integer as overallfastest,
                null::integer as personalfastest,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        value as lap_time,
        status as lap_time_status,
        overallfastest as is_overall_fastest,
        personalfastest as is_personal_fastest,
        driver,
        _streamtimestamp as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_last_lap
)

select *
from formatted

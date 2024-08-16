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

computed as (
    select
        driver as car_number,
        {{ varchar_lap_time_to_interval("value") }} as lap_time,
        status as lap_time_status,
        overallfastest as is_overall_fastest,
        personalfastest as is_personal_fastest,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_last_lap
    where
        value is not null
        and len(value) > 0
),

formatted as (
    select
        session_id,
        car_number,
        lap_time,
        lap_time_status,
        is_personal_fastest,
        session_ts
    from computed
)

select *
from formatted

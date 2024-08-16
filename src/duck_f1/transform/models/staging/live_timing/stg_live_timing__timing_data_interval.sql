with
raw_timing_data_interval as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_interval"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_interval") }}

    {% else %}

            select
                null::integer as value,
                null::integer as catching,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        value as next_car_interval,
        catching as is_catching,
        driver,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_interval
)

select *
from formatted

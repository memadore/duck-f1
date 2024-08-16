with
raw_timing_data_speeds as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_speeds"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_speeds") }}

    {% else %}

            select
                null::integer as speedkey,
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
        speedkey as speed_key,
        value as speed_value,
        status as speed_status,
        overallfastest as is_overall_fastest,
        personalfastest as is_personal_fastest,
        driver,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_speeds
)

select *
from formatted

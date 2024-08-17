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

computed as (
    select
        driver as car_number,
        speedkey as speed_key,
        if(len(value) > 0, value::integer, null) as speed_value,
        status as speed_status,
        coalesce(overallfastest::boolean, false) as is_overall_fastest,
        coalesce(personalfastest::boolean, false) as is_personal_fastest,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_speeds
    where speed_value is not null
),

formatted as (
    select
        session_id,
        car_number,
        speed_key,
        speed_value,
        is_overall_fastest
            as is_personal_fastest,
        speed_status,
        session_ts
    from computed
)

select *
from formatted

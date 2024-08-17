with
raw_timing_data_sectors as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_sectors"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_data_sectors") }}

    {% else %}

            select
                null::integer as sectorkey,
                null::integer as stopped,
                null::integer as value,
                null::integer as previousvalue,
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
        sectorkey::integer as sector_key,
        stopped::boolean as is_stopped,
        to_milliseconds(if(len(value) > 0, value::numeric * 1000, null)) as sector_time,
        to_milliseconds(if(len(previousvalue) > 0, previousvalue::numeric * 1000, null))
            as previous_value,
        status as sector_status,
        coalesce(overallfastest::boolean, false) as is_overall_fastest,
        coalesce(personalfastest::boolean, false) as is_personal_fastest,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_sectors
    where sector_time is not null
),

formatted as (
    select
        session_id,
        car_number,
        sector_key,
        sector_time,
        previous_value,
        is_overall_fastest,
        is_personal_fastest,
        sector_status,
        is_stopped,
        session_ts
    from computed
)

select *
from formatted

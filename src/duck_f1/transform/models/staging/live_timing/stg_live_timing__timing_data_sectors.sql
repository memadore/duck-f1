with
raw_timing_data_sectors as (
        {% if check_if_source_exists(
            "ing__live_timing", "live_timing__timing_data_sectors"
        ) | trim == "True" %}

        select *
        from {{ source("ing__live_timing", "live_timing__timing_data_sectors") }}

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

formatted as (
    select
        sectorkey as sector_key,
        stopped as is_stopped,
        value as sector_time,
        previousvalue as previous_value,
        status as sector_status,
        overallfastest as is_overall_fastest,
        personalfastest as is_personal_fastest,
        driver,
        _streamtimestamp as _stream_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_sectors
)

select *
from formatted

with
raw_timing_stats_sectors as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_stats_sectors"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__timing_stats_sectors") }}

    {% else %}

            select
                null::integer as sectorkey,
                null::integer as value,
                null::integer as position,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        sectorkey as sector_key,
        value as sector_time,
        position,
        driver,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_stats_sectors
)

select *
from formatted

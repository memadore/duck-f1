with
raw_timing_data_sector_segments as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__timing_data_sector_segments"
        ) | trim == "True" %}

        select *
        from
                {{
                    source(
                        "src_live_timing", "live_timing__timing_data_sector_segments"
                    )
                }}

    {% else %}

            select
                null::integer as sectorkey,
                null::integer as segmentkey,
                null::integer as status,
                null::integer as driver,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        sectorkey as sector_key,
        segmentkey as segment_key,
        status as sector_status,
        driver,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_timing_data_sector_segments
)

select *
from formatted

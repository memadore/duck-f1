with
raw_extrapolated_clock as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__extrapolated_clock"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__extrapolated_clock") }}

    {% else %}

            select
                null::integer as utc,
                null::integer as remaining,
                null::integer as extrapolating,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        utc as utc_ts,
        remaining as remaining_session_time,
        extrapolating as is_extrapolated,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_extrapolated_clock
)

select *
from formatted

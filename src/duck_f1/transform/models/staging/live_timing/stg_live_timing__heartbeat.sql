with
raw_heartbeat as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__heartbeat"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__heartbeat") }}

    {% else %}

            select
                null::integer as utc,
                null::integer as interval,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        utc::timestamp as utc_ts,
        _streamtimestamp::interval as session_ts,
        utc_ts - session_ts as start_utc,
        {{ live_timing__metadata() }}
    from raw_heartbeat
)

select *
from formatted

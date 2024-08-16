with
raw_tla_rcm as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__tla_rcm"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__tla_rcm") }}

    {% else %}

            select
                null::integer as timestamp,
                null::integer as message,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        timestamp as event_local_ts,
        message as race_control_message,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_tla_rcm
)

select *
from formatted

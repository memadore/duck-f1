with
raw_session_status as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__session_status"
        ) | trim == "True" %}

        select *
        from {{ source("src_live_timing", "live_timing__session_status") }}

    {% else %}

            select
                null::integer as status,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        status as session_status,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }},
        session_ts as status_start_ts,
        lead(session_ts) over (partition by session_id order by session_ts) as status_end_ts
    from raw_session_status
)

select *
from formatted

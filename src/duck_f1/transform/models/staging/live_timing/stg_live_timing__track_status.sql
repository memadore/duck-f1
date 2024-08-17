with
raw_track_status as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__track_status"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__track_status") }}

    {% else %}

            select
                null::integer as status,
                null::integer as message,
                null::integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

renamed as (
    select
        status as _track_status_id,
        message as status_message,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_track_status
),

computed as (
    select
        *,
        session_ts as status_start_ts,
        lead(session_ts) over (partition by session_id order by session_ts) as status_end_ts,
        status_end_ts - status_start_ts as status_duration
        -- TODO: Add session end
    from renamed
),

formatted as (
    select
        session_id,
        _track_status_id,
        status_message,
        session_ts,
        status_start_ts,
        status_end_ts,
        status_duration
    from computed
)

select *
from formatted

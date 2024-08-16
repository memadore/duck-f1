with
raw_audio_streams as (
        {% if check_if_source_exists(
            "src_live_timing", "live_timing__audio_streams"
        ) | trim == "True" %}

        select * from {{ source("src_live_timing", "live_timing__audio_streams") }}

    {% else %}

            select
                null:integer as name,
                null:integer as language,
                null:integer as uri,
                null:integer as path,
                null:integer as utc,
                null:integer as _streamtimestamp,
                {{ live_timing__empty_metadata() }}
            where false

        {% endif %}
),

formatted as (
    select
        name,
        language,
        uri,
        path,
        utc as event_utc_ts,
        _streamtimestamp::interval as session_ts,
        {{ live_timing__metadata() }}
    from raw_audio_streams
)

select *
from formatted

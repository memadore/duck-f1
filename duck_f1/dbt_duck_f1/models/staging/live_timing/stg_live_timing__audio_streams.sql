with
    raw_audio_streams as (select * from {{ source("live_timing", "audio_streams") }}),
    formatted as (
        select
            name as name,
            language as language,
            uri as uri,
            path as path,
            utc as event_utc_ts,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_audio_streams
    )
select *
from formatted

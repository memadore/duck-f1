with
    raw_audio_streams as (select * from {{ source('live_timing', 'audio_streams') }}),
    formatted as (
        select
            Name as name,
            Language as language,
            Uri as uri,
            Path as path,
            Utc as event_ts,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_audio_streams
    )
select *
from formatted
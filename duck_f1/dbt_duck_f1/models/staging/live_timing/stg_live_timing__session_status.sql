with
    raw_session_status as (select * from {{ source('live_timing', 'session_status') }}),
    formatted as (
        select
            ts as ts,
            Status as session_status,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_status
    )
select *
from formatted
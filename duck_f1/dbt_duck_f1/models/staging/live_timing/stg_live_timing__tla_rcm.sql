with
    raw_tla_rcm as (select * from {{ source('live_timing', 'tla_rcm') }}),
    formatted as (
        select
            ts as ts,
            Timestamp as utc_ts,
            Message as race_control_message,
            {{ live_timing__metadata_raw_columns() }}
        from raw_tla_rcm
    )
select *
from formatted
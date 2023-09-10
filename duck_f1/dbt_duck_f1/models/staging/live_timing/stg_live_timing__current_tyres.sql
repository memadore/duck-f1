with
    raw_current_tyres as (select * from {{ source("live_timing", "current_tyres") }}),
    formatted as (
        select
            driver as driver_number,
            compound as tyre_compound,
            new as is_new,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_current_tyres
    )
select *
from formatted

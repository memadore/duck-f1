with
    raw_tyre_stint_series as (
        select * from {{ source("live_timing", "tyre_stint_series") }}
    ),
    formatted as (
        select
            driver as driver_number,
            stint as stint_id,
            compound as tyre_compound,
            new as is_new,
            tyresnotchanged as tyres_not_changed,
            totallaps as total_laps,
            startlaps as start_laps,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_tyre_stint_series
    )
select *
from formatted

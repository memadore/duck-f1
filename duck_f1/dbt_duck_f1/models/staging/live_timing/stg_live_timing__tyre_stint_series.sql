with
    raw_tyre_stint_series as (select * from {{ source('live_timing', 'tyre_stint_series') }}),
    formatted as (
        select
            Driver as driver_number,
            Stint as stint_id,
            Compound as tyre_compound,
            New as is_new,
            TyresNotChanged as tyres_not_changed,
            TotalLaps as total_laps,
            StartLaps as start_laps,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_tyre_stint_series
    )
select *
from formatted
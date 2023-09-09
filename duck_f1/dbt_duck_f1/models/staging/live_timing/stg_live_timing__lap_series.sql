with
    raw_lap_count as (select * from {{ source('live_timing', 'lap_count') }}),
    formatted as (
        select
            Metric as metric_lable,
            Value as metric_value,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_lap_count
    )
select *
from formatted
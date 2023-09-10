with
    raw_timing_stats as (select * from {{ source("live_timing", "timing_stats") }}),
    formatted as (
        select
            driver as driver,
            metricname as metric_name,
            metrickey as metric_key,
            metricvalue as metric_value,
            position as metric_position,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_stats
    )
select *
from formatted

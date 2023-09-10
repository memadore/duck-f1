with
    raw_timing_data_status as (
        select * from {{ source("live_timing", "timing_data_status") }}
    ),
    formatted as (
        select
            driver as driver,
            metricname as metric_name,
            metricvalue as metric_value,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_status
    )
select *
from formatted

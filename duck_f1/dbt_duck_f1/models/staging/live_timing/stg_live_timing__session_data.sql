with
    raw_session_data as (select * from {{ source("live_timing", "session_data") }}),
    formatted as (
        select
            key as serie_key,
            utc as utc_ts,
            metricname as metric_name,
            metricvalue as metric_value,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_data
    )
select *
from formatted

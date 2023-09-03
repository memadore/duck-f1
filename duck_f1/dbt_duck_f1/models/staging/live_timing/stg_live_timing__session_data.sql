with
    raw_session_data as (select * from {{ source('live_timing', 'session_data') }}),
    formatted as (
        select
            Key as serie_key,
            Utc as utc_ts,
            MetricName as metric_name,
            MetricValue as metric_value,
            {{ live_timing__metadata_raw_columns() }}
        from raw_session_data
    )
select *
from formatted
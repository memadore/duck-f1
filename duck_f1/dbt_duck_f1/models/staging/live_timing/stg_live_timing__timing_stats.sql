with
    raw_timing_stats as (select * from {{ source('live_timing', 'timing_stats') }}),
    formatted as (
        select
            Driver as driver,
            MetricName as metric_name,
            MetricKey as metric_key,
            MetricValue as metric_value,
            Position as metric_position,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_stats
    )
select *
from formatted
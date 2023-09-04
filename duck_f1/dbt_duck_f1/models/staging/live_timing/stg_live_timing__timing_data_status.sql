with
    raw_timing_data_status as (select * from {{ source('live_timing', 'timing_data_status') }}),
    formatted as (
        select
            Driver as driver,
            MetricName as metric_name,
            MetricValue as metric_value,
            ts as ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_timing_data_status
    )
select *
from formatted
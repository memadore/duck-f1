with
    raw_lap_count as (select * from {{ source('live_timing', 'lap_count') }}),
    formatted as (
        select
            ts as ts,
            metric as metric_lable,
            value as metric_value,
            {{ live_timing__metadata_raw_columns() }}
        from raw_lap_count
    )
select *
from formatted
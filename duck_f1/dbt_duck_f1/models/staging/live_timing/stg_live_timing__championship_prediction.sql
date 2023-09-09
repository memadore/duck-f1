with
    raw_championship_prediction as (select * from {{ source('live_timing', 'championship_prediction') }}),
    formatted as (
        select
            Entity as entity,
            Identifier as identifier,
            Metric as metric_name,
            Value as metric_value,
            _StreamTimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_championship_prediction
    )
select *
from formatted
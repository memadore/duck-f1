with
    raw_championship_prediction as (
        select * from {{ source("live_timing", "championship_prediction") }}
    ),
    formatted as (
        select
            entity as entity,
            identifier as identifier,
            metric as metric_name,
            value as metric_value,
            _streamtimestamp as _stream_ts,
            {{ live_timing__metadata_raw_columns() }}
        from raw_championship_prediction
    )
select *
from formatted

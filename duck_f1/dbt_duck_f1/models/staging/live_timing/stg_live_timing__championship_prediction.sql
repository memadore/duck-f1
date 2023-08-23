with
    raw_championship_prediction as (select * from {{ source('live_timing', 'championship_prediction') }}),
    formatted as (
        select
            ts as ts,
            entity as entity,
            identifier as identifier,
            metric as metric,
            value as value,
            {{ live_timing__metadata_raw_columns() }}
        from raw_championship_prediction
    )
select *
from formatted
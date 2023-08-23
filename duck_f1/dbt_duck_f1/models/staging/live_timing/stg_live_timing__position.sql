with
    raw_position as (select * from {{ source('live_timing', 'position') }}),
    formatted as (
        select
            ts as ts,
            Timestamp as capture_ts,
            Driver as driver,
            Status as status,
            X as x_position,
            Y as y_position,
            Z as z_position,
            {{ live_timing__metadata_raw_columns() }}
        from raw_position
    )
select *
from formatted
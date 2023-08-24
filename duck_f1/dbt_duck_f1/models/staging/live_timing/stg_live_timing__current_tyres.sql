with
    raw_current_tyres as (select * from {{ source('live_timing', 'current_tyres') }}),
    formatted as (
        select
            ts as ts,
            Driver as driver_number,
            Compound as tyre_compound,
            New as is_new,
            {{ live_timing__metadata_raw_columns() }}
        from raw_current_tyres
    )
select *
from formatted
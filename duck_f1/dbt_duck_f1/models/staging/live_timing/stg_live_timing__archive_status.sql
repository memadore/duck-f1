with
    raw_archive_status as (select * from {{ source("live_timing", "archive_status") }}),
    formatted as (
        select status as status, {{ live_timing__metadata_raw_columns() }}
        from raw_archive_status
    )
select *
from formatted

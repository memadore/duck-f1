with
    raw_status as (select * from {{ source("ergast", "status") }}),
    formatted as (select statusid as ergast_status_id, status as status from raw_status)
select *
from formatted

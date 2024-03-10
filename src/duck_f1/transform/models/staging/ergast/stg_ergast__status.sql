with
    raw_status as (select * from {{ source("ing__ergast", "ergast__status") }}),
    formatted as (
        select
            {{ dbt_utils.generate_surrogate_key(["status"]) }} as status_id,
            statusid as ergast_status_id,
            status as status
        from raw_status
    )
select *
from formatted

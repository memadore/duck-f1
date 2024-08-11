with
raw_status as (select * from {{ source("src_ergast", "ergast__status") }}),

formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(["status"]) }} as status_id,
        statusid as _ergast_status_id,
        status
    from raw_status
)

select *
from formatted

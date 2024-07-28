with
raw_seasons as (select * from {{ source("ing__ergast", "ergast__seasons") }}),

formatted as (
    select
        year,
        url
    from raw_seasons
)

select *
from formatted

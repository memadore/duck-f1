with
raw_seasons as (select * from {{ source("ing__ergast", "ergast__seasons") }}),

formatted as (
    select
        year,
        url,
        md5(year::varchar) as season_id
    from raw_seasons
)

select *
from formatted

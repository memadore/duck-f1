with
raw_seasons as (select * from {{ source("src_ergast", "ergast__seasons") }}),

formatted as (
    select
        year,
        url,
        md5(year::varchar) as season_id
    from raw_seasons
)

select *
from formatted

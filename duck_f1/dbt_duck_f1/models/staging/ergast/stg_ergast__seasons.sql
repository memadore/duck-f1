with
    raw_seasons as (select * from {{ source("ergast", "seasons") }}),
    formatted as (select year as year, url as url from raw_seasons)
select *
from formatted

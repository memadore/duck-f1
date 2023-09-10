with
    raw_constructor_results as (
        select * from {{ source("ergast", "constructor_results") }}
    ),
    formatted as (
        select
            constructorresultsid as ergast_constructor_results_id,
            raceid as ergast_race_id,
            constructorid as ergast_constructor_id,
            points as points,
            status as status
        from raw_constructor_results
    )
select *
from formatted

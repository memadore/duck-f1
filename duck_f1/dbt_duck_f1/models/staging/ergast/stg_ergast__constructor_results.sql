with
    raw_constructor_results as (select * from {{ source('ergast', 'constructor_results') }}),
    formatted as (
        select
            constructorresultsid as constructor_results_id,
            raceid as race_id,
            constructorid as constructor_id,
            points as points,
            status as status
        from raw_constructor_results
    )
select *
from formatted
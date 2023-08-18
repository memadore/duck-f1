with
    raw_constructor_standings as (select * from {{ source('ergast', 'constructor_standings') }}),
    formatted as (
        select
            constructorstandingsid as constructor_standing_id,
            raceid as race_id,
            constructorid as constructor_id,
            points as points,
            position as position,
            positiontext as position_label,
            wins as win_count
        from raw_constructor_standings
    )
select *
from formatted
with
raw_constructor_standings as (
    select * from {{ source("src_ergast", "ergast__constructor_standings") }}
),

constructor_ids as (
    select
        constructor_id,
        _ergast_constructor_id
    from {{ ref("stg_ergast__constructors") }}
),

session_ids as (
    select
        session_id,
        _ergast_race_id
    from {{ ref("stg_sessions") }}
    where session_type = 'race'
),

formatted as (
    select
        constructor.constructor_id,
        _session.session_id,
        constructor_standings.points,
        constructor_standings.position,
        constructor_standings.positiontext as position_label,
        constructor_standings.wins as win_count
    from raw_constructor_standings as constructor_standings
    inner join
        constructor_ids as constructor
        on constructor_standings.constructorid = constructor._ergast_constructor_id
    inner join session_ids as _session on constructor_standings.raceid = _session._ergast_race_id
)

select *
from formatted

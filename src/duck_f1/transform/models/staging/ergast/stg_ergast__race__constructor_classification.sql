with
raw_constructor_results as (
    select * from {{ source("src_ergast", "ergast__constructor_results") }}
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
        constructor_result.points,
        if(constructor_result.status = '\N', null, constructor_result.status) as status
    from raw_constructor_results as constructor_result
    inner join
        constructor_ids as constructor
        on constructor_result.constructorid = constructor._ergast_constructor_id
    inner join session_ids as _session on constructor_result.raceid = _session._ergast_race_id
)

select *
from formatted

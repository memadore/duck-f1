select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__circuits"),
            except=["_ergast_circuit_id"],
        )
    }}
from {{ ref("stg_ergast__circuits") }}

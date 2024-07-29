select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__circuits"),
            except=["ergast_circuit_id"],
        )
    }}
from {{ ref("stg_ergast__circuits") }}

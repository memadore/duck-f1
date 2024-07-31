select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__constructors"),
            except=["ergast_constructor_id"],
        )
    }}
from {{ ref("stg_ergast__constructors") }}

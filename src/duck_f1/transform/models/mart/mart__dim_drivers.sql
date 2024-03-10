select
    {{
        dbt_utils.star(
            from=ref("stg_ergast__drivers"),
            except=["ergast_driver_id", "driver_reference"],
        )
    }}
from {{ ref("stg_ergast__drivers") }}

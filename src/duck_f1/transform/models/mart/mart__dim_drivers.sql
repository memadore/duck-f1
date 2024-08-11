select
    {{
        dbt_utils.star(
            from=ref("int_drivers"),
            except=[
                "_ergast_driver_id",
                "_live_timing_driver_id",
                "_full_name_key"
            ]
        )
    }}
from {{ ref("int_drivers") }}

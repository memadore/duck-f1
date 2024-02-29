from dagster import Definitions

from .assets import all_assets
from .jobs import dbt_build_job, ergast_job, live_timing_job
from .resources import FileSystemResource, init_resources

definitions = Definitions(
    assets=all_assets,
    jobs=[dbt_build_job, ergast_job, live_timing_job],
    resources=init_resources(FileSystemResource()),
)

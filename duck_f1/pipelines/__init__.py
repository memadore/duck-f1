import os

from dagster import Definitions

from .assets import all_assets
from .jobs import ergast_job
from .resources import RESOURCES

ENV = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(assets=all_assets, jobs=[ergast_job], resources=RESOURCES)

import os
from pathlib import Path

from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "transform").resolve()

if all(
    [
        int(os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD", "1")),
        not int(os.getenv("DAGSTER_DBT_PROJECT_IS_CACHED", "0")),
    ]
):
    dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))
    dbt_parse_invocation = dbt.cli(["parse"], target_path=Path("target")).wait()
    DBT_MANIFEST_PATH = dbt_parse_invocation.target_path.joinpath("manifest.json")
    os.environ["DAGSTER_DBT_PROJECT_IS_CACHED"] = "1"
else:
    DBT_MANIFEST_PATH = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

import json
from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from ...constants import DBT_MANIFEST_PATH
from ...resources import FileSystemResource


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return super().get_asset_key(dbt_resource_props).with_prefix("duckdb")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "duckdb"


dagster_dbt_translator = CustomDagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=dagster_dbt_translator,
)
def duckdb_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource, fs_config: FileSystemResource
):
    context.log.info("dbt manifest path: %s", DBT_MANIFEST_PATH)
    var = {"db_dir": fs_config.output_path, "db_name": fs_config.db_name}
    dbt_run_args = ["run", "--vars", json.dumps(var), "--target", "dist"]
    context.log.info("Run args: %s", dbt_run_args)
    yield from dbt.cli(dbt_run_args, context=context).stream()

from pathlib import Path
from typing import Dict, List

import yaml
from dagster import AssetOut, AssetsDefinition

from .config import ErgastAsset, ErgastConfig
from .duckdb import duckdb_parquet_asset_factory
from .parquet import parquet_asset_factory


class ErgastAssetsManager:
    def __init__(self):
        self._config = self._load_yaml_config()
        self._multi_asset_outs = self._create_multi_asset_outs()

    def _load_yaml_config(self) -> ErgastConfig:
        config_path = (Path(__file__).parent / "values.yaml").resolve()
        with config_path.open("r", encoding="utf-8") as file:
            return ErgastConfig(**yaml.safe_load(file))

    @property
    def assets(self) -> List[ErgastAsset]:
        return self._config.assets

    @property
    def multi_asset_outs(self) -> Dict[str, AssetOut]:
        return self._multi_asset_outs

    @property
    def url(self) -> str:
        return self._config.url

    def create_parquet_assets(self) -> List[AssetsDefinition]:
        return [
            parquet_asset_factory(
                multi_asset_out=self._multi_asset_outs,
                ergast_assets=self.assets,
                url=self.url,
            )
        ]

    def create_duckdb_assets(self) -> List[AssetsDefinition]:
        return [duckdb_parquet_asset_factory(i) for i in self.assets]

    def _create_multi_asset_outs(self) -> Dict[str, AssetOut]:
        default_config = {
            "key_prefix": ["ergast"],
            "is_required": False,
            "io_manager_key": "pyarrow_parquet_io_manager",
        }

        return {i.asset_name: AssetOut(**default_config) for i in self._config.assets}

import io
import urllib.request
import zipfile
from typing import Dict, List

from dagster import AssetExecutionContext, AssetOut, Output, multi_asset
from pyarrow import Table, csv

from .config import ErgastAsset


def ergast_asset_factory(
    multi_asset_out: Dict[str, AssetOut], ergast_assets: List[ErgastAsset], url: str
):
    @multi_asset(
        outs=multi_asset_out,
        can_subset=True,
        group_name="ergast",
        compute_kind="python",
    )
    def _parquet_files(context: AssetExecutionContext) -> Table:
        with urllib.request.urlopen(url) as dl_file:
            with zipfile.ZipFile(io.BytesIO(dl_file.read())) as archive:
                for i in ergast_assets:
                    with archive.open(i.file) as file:
                        table = csv.read_csv(file)
                        yield Output(value=table, output_name=i.asset_name)

    return _parquet_files

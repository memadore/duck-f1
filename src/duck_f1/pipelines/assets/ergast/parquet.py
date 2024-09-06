import io
import urllib.request
import zipfile
from typing import Dict, List

import pyarrow as pa
from dagster import AssetExecutionContext, AssetOut, Output, multi_asset
from pyarrow import Table, csv
from pyarrow.csv import ConvertOptions

from .config import ErgastAsset


def parquet_asset_factory(
    multi_asset_out: Dict[str, AssetOut], ergast_assets: List[ErgastAsset], url: str
):
    @multi_asset(
        outs=multi_asset_out,
        can_subset=True,
        group_name="ergast",
        compute_kind="python",
    )
    def _parquet_files(context: AssetExecutionContext) -> Table:
        schemas = {
            "pit_stops": pa.schema(
                [
                    ("raceId", pa.int16()),
                    ("driverId", pa.int16()),
                    ("stop", pa.int16()),
                    ("lap", pa.int16()),
                    ("time", pa.string()),
                    ("duration", pa.string()),
                    ("milliseconds", pa.int32()),
                ]
            )
        }
        with urllib.request.urlopen(url) as dl_file:
            with zipfile.ZipFile(io.BytesIO(dl_file.read())) as archive:
                for i in ergast_assets:
                    with archive.open(i.file) as file:
                        if i.asset_name in schemas:
                            table = csv.read_csv(
                                file,
                                convert_options=ConvertOptions(
                                    column_types=schemas.get(i.asset_name)
                                ),
                            )
                        else:
                            table = csv.read_csv(file)
                        yield Output(value=table, output_name=i.asset_name)

    return _parquet_files

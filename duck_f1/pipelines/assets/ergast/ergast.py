import io
import urllib.request
import zipfile

import yaml
from dagster import AssetOut, OpExecutionContext, Output, multi_asset
from pyarrow import csv

with open("./config/ergast.yaml", "r", encoding="UTF-8") as stream:
    CONFIG = yaml.safe_load(stream)

json_asset_config = {
    "key_prefix": ["ergast"],
    "is_required": False,
    "io_manager_key": "pyarrow_parquet_io_manager",
}

ergast_assets = {i["table"]: AssetOut(**json_asset_config) for i in CONFIG["tables"]}


@multi_asset(
    outs=ergast_assets,
    can_subset=True,
    group_name="ergast",
    compute_kind="pyarrow",
)
def ergast_tables(context: OpExecutionContext) -> io.BytesIO:
    with urllib.request.urlopen(CONFIG["url"]) as dl_file:
        with zipfile.ZipFile(io.BytesIO(dl_file.read())) as archive:
            context.log.info(archive.filelist)
            for t in CONFIG["tables"]:
                with archive.open(t["file"]) as file:
                    table = csv.read_csv(file)
                    yield Output(value=table, output_name=t["table"])

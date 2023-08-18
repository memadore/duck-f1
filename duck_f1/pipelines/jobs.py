from dagster import AssetSelection, define_asset_job

ergast_job = define_asset_job(name="ergast", selection=AssetSelection.groups("ergast"))

import click
from dagster import DagsterInstance
from rich.progress import track

from .pipelines import definitions
from .pipelines.assets import live_timing_asset_manager
from .pipelines.resources import FileSystemResource, init_resources


@click.group()
@click.option(
    "-o",
    "--output-dir",
    "output_dir",
    type=str,
    default="data",
    help="Output directory for the data warehouse",
)
@click.option("--db-name", type=str, default="f1")
@click.pass_context
def cli(ctx, output_dir, db_name):
    ctx.ensure_object(dict)
    ctx.obj["output_dir"] = output_dir
    ctx.obj["db_name"] = db_name


@cli.command()
@click.option(
    "--season",
    multiple=True,
    type=int,
    help="Filter by season",
)
@click.option("--event-sha", multiple=True, type=str, help="Filter by event sha")
@click.option(
    "--event-location", multiple=True, type=str, help="Filter by event location"
)
@click.option(
    "--event-country", multiple=True, type=str, help="Filter by event country"
)
@click.option("--session-sha", multiple=True, type=str, help="Filter by session sha")
@click.option("--session-type", multiple=True, type=str, help="Filter by session type")
@click.pass_context
def run(
    ctx, season, event_sha, event_location, event_country, session_sha, session_type
):
    """
    Build the duckdb warehouse
    """
    target_sessions = live_timing_asset_manager.session_manager.filter_sessions(
        season=[*season],
        event_sha=[*event_sha],
        event_location=[*event_location],
        event_country=[*event_country],
        session_sha=[*session_sha],
        session_type=[*session_type],
    )

    dagster_instance = DagsterInstance.ephemeral(
        settings={"python_logs": {"python_log_level": "ERROR"}}
    )

    resource_config = FileSystemResource(
        output_dir=ctx.obj["output_dir"], db_name=ctx.obj["db_name"]
    )

    for i in track(
        target_sessions, f"Downloading {len(target_sessions)} sessions datasets ..."
    ):
        config = {
            "execution": {
                "config": {
                    "multiprocess": {
                        "start_method": {
                            "forkserver": {},
                        },
                        "max_concurrent": 8,
                    },
                }
            },
        }

        definitions.get_job_def("live_timing").execute_in_process(
            run_config=config,
            instance=dagster_instance,
            resources=init_resources(resource_config),
            partition_key=i.session_key,
        )

    for i in track(range(1), "Downloading ergast datasets ......."):
        definitions.get_job_def("ergast").execute_in_process(
            instance=dagster_instance, resources=init_resources(resource_config)
        )

    for i in track(range(1), "Building database assets .........."):
        definitions.get_job_def("dbt_build").execute_in_process(
            instance=dagster_instance, resources=init_resources(resource_config)
        )


if __name__ == "__main__":
    cli(obj={})

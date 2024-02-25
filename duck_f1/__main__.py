from pipelines import definitions
from pipelines.assets.live_timing import sessions_manager

if __name__ == "__main__":
    target_sessions = sessions_manager.filter_sessions(season=[2020], session_type=["race"])
    for i in target_sessions:
        print(i.event_path)
    a = definitions.get_job_def("live_timing").all_node_defs
    for i in target_sessions:
        config = {
            "ops": {j.name: {"config": {"session_key": i.session_key}} for j in a},
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
        definitions.get_job_def("live_timing").execute_in_process(run_config=config)

    # definitions.get_job_def("ergast").execute_in_process()
    # definitions.get_job_def("dbt_build").execute_in_process()

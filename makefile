include .env
export

dev: dagster-reset py-clean dagster-dev-server

dagster-dev-server:
	poetry run dagster dev -w workspace.yaml

dagster-reset:
	rm -rf ${HOME}/.dagster
	mkdir -p ${HOME}/.dagster
	cp .devcontainer/config/dagster.yaml ${HOME}/.dagster

dbt-build:
	cd ./duck_f1/dbt_duck_f1; \
	poetry run dbt build

job-live-timing:
	poetry run dagster job backfill -d duck_f1 -m pipelines -j live_timing --partitions 2020/11/29/race --noprompt

py-clean:
	find ./duck_f1/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	find ./tests/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	rm -f .coverage
	rm -rf .pytest_cache

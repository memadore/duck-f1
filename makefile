include .env
export

dev: dagster-reset py-clean dbt-compile dagster-dev-server

dagster-dev-server:
	poetry run dagster dev -w workspace.yaml -p 3001

dagster-reset:
	rm -rf ${HOME}/.dagster
	mkdir -p ${HOME}/.dagster
	cp .devcontainer/config/dagster.yaml ${HOME}/.dagster

dbt-build:
	cd ./duck_f1/transform; \
	poetry run dbt build --vars '{db_dir: "../../data", db_name: f1}' --target dist;

dbt-compile:
	cd ./duck_f1/transform; \
	poetry run dbt compile --vars '{db_dir: "../../data", db_name: f1}' --target dist;

dbt-parse:
	cd ./duck_f1/transform; \
	poetry run dbt parse --vars '{db_dir: "../../data", db_name: f1}' --target dist;

duck-f1-weekend:
	poetry run duck-f1 run --event-sha a3cf4bf0

job-ergast:
	poetry run dagster job execute -d duck_f1 -m pipelines -j ergast

job-live-timing:
	poetry run dagster job execute -d duck_f1 -m pipelines -j live_timing

py-clean:
	find ./duck_f1/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	find ./tests/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	rm -f .coverage
	rm -rf .pytest_cache

py-tests:
	poetry run coverage run -m pytest tests -v --junitxml=report.xml && poetry run coverage xml


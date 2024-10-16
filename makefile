include .env
export

dev: dagster-reset py-clean dbt-compile dagster-dev-server

dagster-dev-server:
	pdm run dagster dev -w workspace.yaml -p 3001

dagster-reset:
	rm -rf ${HOME}/.dagster
	mkdir -p ${HOME}/.dagster
	cp .devcontainer/config/dagster.yaml ${HOME}/.dagster

dbt-build:
	cd ./src/duck_f1/transform; \
	pdm run dbt build;

dbt-codegen:
	cd ./src/duck_f1/transform; \
	dbt-coves generate properties --update-strategy update --no-prompt

dbt-compile:
	cd ./src/duck_f1/transform; \
	pdm run dbt compile;

dbt-deps:
	cd ./src/duck_f1/transform; \
	pdm run dbt deps

dbt-docs:
	cd ./src/duck_f1/transform; \
	pdm run dbt docs generate --static

dbt-parse:
	cd ./src/duck_f1/transform; \
	pdm run dbt parse;

dbt-run:
	cd ./src/duck_f1/transform; \
	pdm run dbt run;

dbt-test:
	cd ./src/duck_f1/transform; \
	pdm run dbt test;

duck-f1-weekend:
	pdm run duck-f1 run --event-sha a3cf4bf0

job-ergast:
	pdm run dagster job execute -d duck_f1 -m pipelines -j ergast

job-live-timing:
	pdm run dagster job execute -d duck_f1 -m pipelines -j live_timing

py-clean:
	find ./src/duck_f1/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	find ./tests/ -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	rm -f .coverage coverage.xml report.xml
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .pdm_build

py-publish:
	pdm publish \
		--username ${PYPI_USERNAME} \
		--password ${PYPI_PASSWORD}

py-publish-test:
	pdm publish --repository testpypi \
		--username ${PYPI_TEST_USERNAME} \
		--password ${PYPI_TEST_PASSWORD}

py-release:
	semantic-release version

py-tests:
	pdm run coverage run -m pytest tests -v --junitxml=report.xml && pdm run coverage xml

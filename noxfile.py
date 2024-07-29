import os

import nox
from nox import Session

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})
python_versions = ["3.8", "3.9", "3.10", "3.11"]

dbt_variables = {
    "DBT_PROJECT_DIR": "src/duck_f1/transform",
    "DBT_PROFILES_DIR": "src/duck_f1/transform",
    "DBT_PROFILE": "dev",
}


@nox.session
def lint(session: Session):
    session.run_always("pdm", "install", "-G", "lint", external=True)
    session.run("ruff", "format", "--check", "./src/duck_f1/pipelines")


@nox.session(python=python_versions)
def tests(session: Session):
    session.run_always("pdm", "install", "-G", "test", external=True)
    session.run("pytest", "tests", "-v")


@nox.session(python=python_versions)
def parse(session: Session):
    session.run_always("pdm", "install", "--prod", external=True)
    session.run("dbt", "deps", env=dbt_variables)
    session.run("dbt", "parse", env=dbt_variables)


@nox.session(python=python_versions)
def run(session: Session):
    session.run_always("pdm", "install", "--prod", external=True)
    session.run("duck-f1", "run", "--event-sha", "a3cf4bf0")

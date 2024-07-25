ARG PYTHON_VERSION=3.11

FROM "mcr.microsoft.com/devcontainers/python:${PYTHON_VERSION}"

ARG DEBIAN_FRONTEND=noninteractive
ARG USER=vscode

# Python and poetry installation
USER $USER
ARG HOME="/home/$USER"

RUN sudo apt-get update && sudo apt-get install -y pipx && pipx ensurepath

# Poetry
RUN pipx install poetry dbt-core

RUN poetry config virtualenvs.in-project true \
    && poetry config installer.max-workers 4 \
    && poetry self add poetry-plugin-up

# tools
RUN pip install --upgrade pip \
    && pip install --user 'shandy-sqlfmt[jinjafmt]'
# /bin/bash
sudo apt-get update
sudo apt-get install -y make build-essential

sudo apt-get install -y \
    curl libbz2-dev libffi-dev liblzma-dev libncursesw5-dev libreadline-dev libsqlite3-dev libssl-dev libxml2-dev libxmlsec1-dev tk-dev xz-utils zlib1g-dev

sudo curl https://pyenv.run | bash \
    && pyenv update \
    && pyenv install "3.8" "3.9" "3.10" "3.11" "3.12" \
    && pyenv global "3.8" "3.9" "3.10" "3.11" "3.12" \
    && pyenv rehash

python3 -m ensurepip --upgrade

python3 -m pip install --user pipx
python3 -m pipx ensurepath

pipx install \
    pdm \
    pre-commit \
    dbt-coves \
    python-semantic-release

pipx inject dbt-coves dbt-duckdb

make py-clean
pdm install -G:all

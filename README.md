# DuckF1

Hello, and welcome to DuckF1! It’s a collection of datasets for all things related to Formula 1. It uses parquets files and DuckDB for storage and compute, making it available on every platform and can queried in almost any programming language.

> [!WARNING]
> This project is in active development and does not guarantee compatibility between minor versions until the first major release. The table definitions are prone to changes and you might need to adapt your custom queries.

## Installation

DuckF1 is available on PyPi and support Python 3.9 through version 3.11. It is strongly recommended that you install the duck-f1 cli with `pipx`. It will create its own dedicated environment while making the project available globally.

```bash
pipx install duck-f1
```

## Building the database

To get started on generating the database, you can execute the `duck-f1 run` command. By default, it will capture the historical data as well as the telemetry data for the last Grand Prix weekend.

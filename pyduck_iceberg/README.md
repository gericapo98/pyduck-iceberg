# pyduck-iceberg

`pyduck-iceberg` is a small Python project that demonstrates how to work with **Apache Iceberg** tables using a Python-based stack (and optionally other analytical tools such as DuckDB), packaged with a `docker-compose` setup to run the environment locally.

> ⚠️ Note: Adjust this description to match the exact purpose of your project (library, demo, service, etc.).

---

## Features

- Spin up a ready-to-use environment via `docker-compose` for experimenting with Apache Iceberg.
- Python package `pyduck_iceberg` containing the core logic for working with Iceberg tables.
- [Optional: add 2–3 bullet points about what the code actually does once you verify it, e.g.]
  - Create and query Iceberg tables in a local environment.
  - Example workflows for reading/writing data via Python.
  - Simple scripts to showcase Iceberg table operations (schema evolution, snapshots, etc.).

---

## Project Structure

- `pyduck_iceberg/` – Python package with the main code.
- `docker-compose.yml` – Docker Compose configuration to start the required services for running and testing the project.

Add or adjust items as needed if you have more files/directories.

---

## Quickstart

### Prerequisites

- Docker and Docker Compose installed
- Python `<version>` (if you run code directly outside of Docker)

### 1. Start the environment

```bash
docker-compose up -d
```

This will start the services defined in `docker-compose.yml` (e.g., catalog, storage, and any supporting components required by the project).

### 2. Run the Python code

If you run Python inside a container:

```bash
docker-compose exec <service_name> python -m pyduck_iceberg
```

If you run it locally (outside Docker):

```bash
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate
pip install -e .
python -m pyduck_iceberg
```

Replace `<service_name>` and commands above with the concrete way your project is meant to be executed (script name, CLI entrypoint, etc.).

---

## Configuration

Configuration is typically controlled via:

- Environment variables defined in `docker-compose.yml`
- [Optional] A configuration file inside `pyduck_iceberg/` (e.g. `config.py` or `.env`)

Document any key variables here, for example:

- `ICEBERG_CATALOG_URI` – URI for the Iceberg catalog
- `WAREHOUSE_PATH` – Path or bucket where Iceberg tables are stored

---

## Development

```bash
git clone https://github.com/gericapo98/pyduck-iceberg.git
cd pyduck-iceberg
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Run tests (adjust to your actual test runner):

```bash
pytest
```

---

## License


FROM python:3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DO_NOT_TRACK=1 \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-extensions
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# DuckDB extensions are installed at build time; the benchmark itself never downloads anything.
RUN python -c "import duckdb; c = duckdb.connect(config={'extension_directory': '/opt/duckdb-extensions'}); [c.execute(f'INSTALL {e}') for e in ('httpfs', 'avro', 'iceberg')]"
COPY bench ./bench
ENTRYPOINT ["python", "-m", "bench.orchestrate"]

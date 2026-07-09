# Bayan backend — FastAPI on gunicorn+uvicorn.
# Single stage: the heavy layers (system ODBC drivers, playwright chromium) are
# runtime deps, so a builder stage would save little. Non-root at the end.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# unixODBC + Microsoft ODBC 18 driver for pyodbc (mirrors
# scripts/install_mssql_odbc_linux.sh, Debian 12 "bookworm" path).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 ca-certificates unixodbc \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Chromium for dashboard snapshot/alert rendering (app/alerts_service.py,
# app/routers/snapshot.py). Browsers land in a shared, root-readable path.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN playwright install --with-deps chromium

COPY . .

# Data volume: SQLite metadata, DuckDB store + duckdb.active + duckdb_tmp, and
# the Prometheus multiprocess dir all live under /data so one volume persists all.
ENV DUCKDB_PATH=/data/local.duckdb \
    METADATA_DB_PATH=/data/meta.sqlite \
    PROMETHEUS_MULTIPROC_DIR=/data/prom_multiproc \
    HOST=0.0.0.0 \
    PORT=8000 \
    RUN_SCHEDULER=1 \
    WEB_CONCURRENCY=1

# Non-root: create user, own /app and the data mount point.
RUN useradd --create-home --uid 10001 bayan \
    && mkdir -p /data \
    && chown -R bayan:bayan /app /data /opt/playwright
USER bayan

VOLUME /data
EXPOSE 8000

# --config gunicorn_conf.py wires the prometheus multiprocess start/reap hooks.
# WEB_CONCURRENCY=1 + RUN_SCHEDULER=1 keeps a single scheduler (see run_prod_gunicorn.sh:31-33).
CMD gunicorn app.main:app \
    --config gunicorn_conf.py \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind ${HOST}:${PORT} \
    --workers ${WEB_CONCURRENCY} \
    --keep-alive 75 \
    --timeout 180 \
    --graceful-timeout 30 \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --access-logfile /dev/null \
    --error-logfile - \
    --log-level warning

FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl \
 && rm -rf /var/lib/apt/lists/*

RUN addgroup --gid 1000 sqlmesh \
  && adduser --uid 1000 --disabled-password sqlmesh --ingroup sqlmesh \
  && mkdir /data \
  && chown sqlmesh:sqlmesh /data

WORKDIR /data
USER sqlmesh
COPY --chown=sqlmesh:sqlmesh . /data
RUN pip install --no-cache-dir "sqlmesh[postgres]"
RUN pip install --no-cache-dir geopandas
RUN pip install --no-cache-dir requests
RUN pip install --no-cache-dir python-dotenv
RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir pyarrow
ENV PATH=/home/sqlmesh/.local/bin:$PATH
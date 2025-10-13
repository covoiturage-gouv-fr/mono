FROM python:3.12-bookworm
RUN addgroup --gid 1000 sqlmesh \
  && adduser --uid 1000 --disabled-password sqlmesh --ingroup sqlmesh \
  && mkdir /data \
  && chown sqlmesh:sqlmesh /data

WORKDIR /data
USER sqlmesh
COPY --chown=sqlmesh:sqlmesh . /data
RUN pip install --no-cache-dir \
    "sqlmesh[postgres]" \
    geopandas \
    requests \
    python-dotenv \
    boto3
ENV PATH=/home/sqlmesh/.local/bin:$PATH
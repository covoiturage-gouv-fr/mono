# syntax=docker/dockerfile:1.7

# Stage 1 -- just: pin the `just` binary, no curl/bash needed in runtime
FROM ghcr.io/casey/just:1.55.1 AS just

# Stage 2 -- builder: compile native wheels into /data/.venv
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# Build deps (stripped in runtime stage):
#   build-essential   gcc/make to compile native Python wheels
#   libpq-dev         libpq headers for psycopg2 build
#   libgdal-dev       GDAL headers for any geo binding build
#   python3-dev       CPython headers for C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      libgdal-dev \
      python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/data/.venv

WORKDIR /data

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv lock --check

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY . /data

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

RUN /data/.venv/bin/dbt deps --project-dir /data

# Stage 3 -- runtime: minimal image, no compilers or dev headers
FROM python:3.13-slim-bookworm AS runtime

# Runtime libs:
#   libpq5            libpq shared lib for psycopg2 / dbt-postgres
#   libgdal32         GDAL shared lib for geo bindings + gdal-bin
#   gdal-bin          ogr2ogr / gdalwarp invoked by dbt macros and pipelines
#   ca-certificates   HTTPS trust store (slim image omits it) for S3 + Postgres TLS
#   git               dbt reads project version from git metadata at startup
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 \
      libgdal32 \
      gdal-bin \
      ca-certificates \
      git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN addgroup --gid 1000 datalake \
  && adduser --uid 1000 --disabled-password --ingroup datalake datalake

COPY --from=just /just /usr/local/bin/just

WORKDIR /data
COPY --from=builder --chown=datalake:datalake /data /data

ENV PATH="/data/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER datalake
ENTRYPOINT []

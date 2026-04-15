FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    gdal-bin \
    libgdal-dev \
    python3-gdal \  
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN addgroup --gid 1000 datalake \
  && adduser --uid 1000 --disabled-password datalake --ingroup datalake \
  && mkdir /data \
  && chown datalake:datalake /data

WORKDIR /data

# uv config
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Vérification lockfile
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv lock --check

# Installation des dépendances
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Code source
COPY --chown=datalake:datalake . /data

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# dbt deps
RUN /data/.venv/bin/dbt deps --project-dir /data

ENV PATH="/data/.venv/bin:$PATH"
ENTRYPOINT []
USER datalake
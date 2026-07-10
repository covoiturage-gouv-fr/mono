# syntax=docker/dockerfile:1.7
# Image de l'API observatoire (api-datalake). Contexte de build = ./api-datalake

# Stage 1 -- builder : compile les wheels natifs dans /app/.venv
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# libpq-dev / build-essential : compilation d'éventuels wheels natifs (psycopg fallback)
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv lock --check

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Stage 2 -- runtime : image minimale, pas de compilateurs
FROM python:3.13-slim-bookworm AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN addgroup --gid 1000 datalake \
  && adduser --uid 1000 --disabled-password --ingroup datalake datalake

WORKDIR /app
COPY --from=builder --chown=datalake:datalake /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

USER datalake
EXPOSE 8081
# --no-server-header : masque la bannière `Server: uvicorn` (durcissement API8/API9).
ENTRYPOINT ["uvicorn", "api_datalake.main:app", "--app-dir", "src", "--host", "0.0.0.0", "--port", "8081", "--no-server-header"]

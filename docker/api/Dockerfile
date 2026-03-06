ARG DENO_VERSION=2.6.4
ARG BIN_IMAGE=denoland/deno:bin-${DENO_VERSION}
FROM ${BIN_IMAGE} AS bin
FROM ubuntu:24.04

RUN useradd --uid 1993 --user-group deno \
  && mkdir /app/ \
  && chown deno:deno /app/

ENV DENO_DIR=/app/
ENV DENO_INSTALL_ROOT=/usr/local

ARG DENO_VERSION
ENV DENO_VERSION=${DENO_VERSION}
COPY --from=bin /deno /usr/bin/deno

RUN apt update && apt install -y p7zip bash curl

# Install just from a specific version
RUN mkdir -p /usr/local/bin
RUN curl --proto '=https' --tlsv1.2 -sSfL https://github.com/casey/just/releases/download/1.40.0/just-1.40.0-x86_64-unknown-linux-musl.tar.gz -o just.tar.gz
RUN echo "181b91d0ceebe8a57723fb648ed2ce1a44d849438ce2e658339df4f8db5f1263 just.tar.gz" | sha256sum --check
RUN tar -xzf just.tar.gz -C /usr/local/bin
RUN rm just.tar.gz
RUN chmod +x /usr/local/bin/just

RUN chown -R deno:deno /app/

USER deno

COPY --chown=deno:deno ./api /app/api

WORKDIR /app/api

RUN just cache
RUN just cache-migrator

CMD ["just", "serve"]

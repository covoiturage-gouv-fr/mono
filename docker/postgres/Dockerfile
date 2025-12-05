FROM postgis/postgis:16-3.5-alpine
# Dépendances de build
RUN apk add --no-cache \
    git \
    build-base \
    cmake \
    postgresql16-dev \
    llvm15-dev \
    clang15

# Récupération et compilation de h3-pg
RUN git clone https://github.com/zachasme/h3-pg.git /tmp/h3-pg \
    && cd /tmp/h3-pg \
    && make PG_CONFIG=/usr/bin/pg_config \
    && make install PG_CONFIG=/usr/bin/pg_config \
    && rm -rf /tmp/h3-pg

# Nettoyage des dépendances de build (optionnel mais recommandé)
RUN apk del \
    git \
    build-base \
    cmake \
    postgresql16-dev \
    llvm15-dev \
    clang15

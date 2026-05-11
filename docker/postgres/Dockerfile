FROM postgis/postgis:16-3.5-alpine
# Dépendances de build
RUN apk add --no-cache \
    git \
    build-base \
    cmake \
    postgresql16-dev \
    llvm21-dev \
    clang21

# Récupération et compilation de h3-pg
# v4.2.3 release commit a26630b8353d441e6bc8065c0a8dcaa3d89ef87b
# https://github.com/zachasme/h3-pg
RUN git clone https://github.com/zachasme/h3-pg.git /tmp/h3-pg \
    && cd /tmp/h3-pg \
    && git checkout a26630b8353d441e6bc8065c0a8dcaa3d89ef87b \
    && make PG_CONFIG=/usr/bin/pg_config \
    && make install PG_CONFIG=/usr/bin/pg_config \
    && rm -rf /tmp/h3-pg

# Nettoyage des dépendances de build (optionnel mais recommandé)
RUN apk del \
    git \
    build-base \
    cmake \
    postgresql16-dev \
    llvm21-dev \
    clang21

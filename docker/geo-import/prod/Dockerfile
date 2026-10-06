# Outils de l'import annuel des périmètres geo (api/src/db/geo/import.sh). Contexte de build = racine.
# Clients PostgreSQL seuls (pas de serveur ni de gosu, sortis du scan Trivy) ; pg_restore doit être
# >= au pg_dump du datalake et à la version majeure du serveur (17).
FROM alpine:3.22

# postgresql17-client crée l'utilisateur postgres (uid 70).
RUN apk add --no-cache bash curl postgresql17-client

COPY api/src/db/geo/check.sql api/src/db/geo/report.sql api/src/db/geo/import.sql api/src/db/geo/import.sh api/src/db/geo/RUNBOOK.md /opt/geo-import/
RUN ln -s /opt/geo-import/import.sh /usr/local/bin/geo-import

# Origine du bucket des dumps (`just export-perimeters`) ; le chemin geo/ est dans import.sh.
ENV GEO_IMPORT_S3_BASE_URL=https://datalake-production.s3.fr-par.scw.cloud

USER postgres
WORKDIR /opt/geo-import
ENTRYPOINT ["/bin/bash"]

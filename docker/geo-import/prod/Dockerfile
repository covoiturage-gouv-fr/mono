# Outils de l'import annuel des périmètres geo (api/src/db/geo/import.sh). Contexte de build = racine.
# pg_restore doit être >= au pg_dump du datalake et à la version majeure du serveur.
FROM postgres:17-alpine

RUN apk add --no-cache bash curl

COPY api/src/db/geo/check.sql api/src/db/geo/report.sql api/src/db/geo/import.sql api/src/db/geo/import.sh api/src/db/geo/RUNBOOK.md /opt/geo-import/
RUN ln -s /opt/geo-import/import.sh /usr/local/bin/geo-import

USER postgres
WORKDIR /opt/geo-import
ENTRYPOINT ["/bin/bash"]

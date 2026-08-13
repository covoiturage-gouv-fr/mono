"""Réindexe le périmètre géographique dans Meilisearch (index `geo`).

Pendant côté datalake de la commande API `territory:index` : à lancer après chaque
(re)construction de `zone_trusted.perimeters` (`just pipeline-trusted-geo`), pour que
la recherche de territoires reste alignée sur le dernier millésime chargé.
"""

import psycopg
import typer
from dotenv import load_dotenv

from pipelines.helpers.meilisearch_client import build_config, index_documents
from pipelines.helpers.meilisearch_query import GEO_DOCUMENTS_SQL
from pipelines.helpers.pg import pg_conninfo

load_dotenv()
app = typer.Typer()


@app.command()
def run():
    config = build_config()
    if config is None:
        print("ℹ️ APP_MEILISEARCH_HOST absent — pas de réindexation")
        return

    with psycopg.connect(pg_conninfo()) as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(GEO_DOCUMENTS_SQL)
        documents = cur.fetchall()

    count = index_documents(config, documents)
    print(f"✅ {count} territoires indexés dans Meilisearch (index `{config['index']}`)")


if __name__ == "__main__":
    app()

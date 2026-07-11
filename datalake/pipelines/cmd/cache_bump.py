"""Invalide le cache de l'API observatoire (INCR obs:cache:version).

À lancer après la (re)construction de la zone exposée : `just cache-bump`, ou enchaîné
par `just pipeline-exposed`.
"""

import typer
from dotenv import load_dotenv

from pipelines.helpers.cache import bump_publication_version

load_dotenv()
app = typer.Typer()


@app.command()
def run():
    bump_publication_version()


if __name__ == "__main__":
    app()

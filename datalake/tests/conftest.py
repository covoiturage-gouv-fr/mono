import pytest

from pipelines.helpers.pg import pg_connect


@pytest.fixture
def pg():
    """Connexion Postgres pour les tests de sortie réelle.

    Skip (au lieu d'échouer) si aucune base n'est configurée : les tests purs
    tournent partout, les tests DB uniquement en CI / avec un Postgres local.
    """
    try:
        conn = pg_connect()
        conn.execute("SELECT 1")
    except Exception:
        pytest.skip("Postgres indisponible (DBT_HOST/PORT/USER/PASSWORD/DBNAME)")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            # Fermeture best-effort au démontage : une connexion déjà coupée
            # (base tombée, timeout) ne doit pas faire échouer le test.
            pass

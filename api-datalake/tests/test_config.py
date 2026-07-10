from psycopg.conninfo import conninfo_to_dict

from api_datalake.config import Settings


def test_conninfo_enforces_read_only_and_statement_timeout():
    s = Settings(dbt_host="db", dbt_user="u", dbt_password="p",
                 dbt_dbname="datalake", db_statement_timeout_ms=1234)
    opts = conninfo_to_dict(s.conninfo())["options"]
    assert "default_transaction_read_only=on" in opts
    assert "statement_timeout=1234" in opts


def test_conninfo_escapes_special_chars_in_password():
    # Un mot de passe avec espace/quote ne doit pas casser la chaîne de connexion.
    s = Settings(dbt_password="p ass'w\\ord")
    parsed = conninfo_to_dict(s.conninfo())
    assert parsed["password"] == "p ass'w\\ord"

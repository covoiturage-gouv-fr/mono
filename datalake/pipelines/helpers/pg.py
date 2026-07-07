import os


def pg_conninfo() -> str:
    return (
        f"host={os.getenv('DBT_HOST')} port={os.getenv('DBT_PORT')} "
        f"user={os.getenv('DBT_USER')} password={os.getenv('DBT_PASSWORD')} "
        f"dbname={os.getenv('DBT_DBNAME')}"
    )

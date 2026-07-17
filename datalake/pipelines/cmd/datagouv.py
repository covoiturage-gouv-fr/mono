"""Publication open-data mensuelle sur data.gouv.fr (job datalake).

Remplace la commande API `export:datagouv`. Pour un mois donné (défaut : le mois
précédent) : garde de complétude, calcul des stats, extraction CSV (COPY, non compressé),
garde « dataset vide », publication de la resource + description, puis rapport JSON dans
`datagouv/logs/` du bucket datalake.

L'upload data.gouv est gaté par `APP_DATAGOUV_UPLOAD` (dry-run par défaut) ; l'ensemble
par `APP_DATAGOUV_ENABLED`.
"""

import json
import os
import tempfile
from datetime import date, datetime, timezone

import psycopg
import typer
from dotenv import load_dotenv

from pipelines.helpers.datagouv_checks import has_failure, render_markdown, run_checks
from pipelines.helpers.datagouv_client import DataGouvClient
from pipelines.helpers.datagouv_query import (
    TEXT_FIELDS,
    build_opendata_copy_sql,
    build_stats_sql,
    csv_header,
    default_window,
)
from pipelines.helpers.datagouv_report import (
    build_description,
    build_report,
    debug_csv_key,
    debug_md_key,
    report_key,
)
from pipelines.helpers.pg import pg_conninfo
from pipelines.helpers.s3 import s3_client, s3_upload

load_dotenv()
app = typer.Typer()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_stats(conn, start: date, end: date, min_occ: int) -> dict:
    sql, params = build_stats_sql(start, end, min_occ)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        row = cur.fetchone()
    return dict(zip(cols, row))


def stream_csv(conn, start: date, end: date, min_occ: int, csv_path: str) -> None:
    inner, params = build_opendata_copy_sql(start, end, min_occ)
    # `;`, NON compressé (contrat data.gouv actuel). En-tête tout-quoté émis à la main
    # (COPY HEADER ne quote pas les colonnes numériques) ; FORCE_QUOTE entoure les
    # valeurs texte de guillemets comme le fichier legacy (NULL restant vide non quoté).
    force_quote = ", ".join(TEXT_FIELDS)
    copy_sql = (
        f"COPY ({inner}) TO STDOUT "
        f"(FORMAT CSV, DELIMITER ';', FORCE_QUOTE ({force_quote}))"
    )
    with open(csv_path, "wb") as f, conn.cursor() as cur:
        f.write((csv_header() + "\n").encode("utf-8"))
        with cur.copy(copy_sql, params) as copy:
            for chunk in copy:
                f.write(chunk)


def assert_not_empty(stats: dict, filename: str) -> None:
    """Refuse de publier un jeu de données vide (amont probablement pas prêt)."""
    if int(stats.get("count_exposed") or 0) < 1:
        raise RuntimeError(
            f"refus de publier un dataset vide {filename} "
            f"(count_exposed={stats.get('count_exposed')}, "
            f"count_total={stats.get('count_total')}) — agrégats du mois pas prêts ?"
        )


def write_report(s3, bucket: str, key: str, report: dict) -> None:
    with tempfile.TemporaryDirectory(prefix="datagouv-report-") as tmp:
        path = os.path.join(tmp, "report.json")
        with open(path, "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        s3_upload(bucket, key, path, client=s3)


@app.command()
def run(
    start: datetime = typer.Option(None, help="Début du mois (YYYY-MM-DD). Défaut : mois précédent."),
    end: datetime = typer.Option(None, help="Fin exclusive (YYYY-MM-DD). Défaut : mois courant."),
    min_occurrences: int = typer.Option(6, help="Seuil k-anonymat sur l'occurrence INSEE."),
    debug: bool = typer.Option(
        False, "--debug",
        help="Ne publie pas ; dépose CSV/description/rapport horodatés sur S3 et imprime le verdict de cohérence.",
    ),
):
    if not os.getenv("APP_DATAGOUV_ENABLED", "").lower() in ("1", "true", "yes", "on"):
        print("⚠️ data.gouv publication DISABLED (APP_DATAGOUV_ENABLED)")
        return

    if start and end:
        d_start, d_end = start.date(), end.date()
    else:
        d_start, d_end = default_window(datetime.now(timezone.utc).date())
    month = d_start.strftime("%Y-%m")
    filename = f"{month}.csv"
    started_at = _now_iso()
    ts = _stamp()

    bucket = os.environ["S3_BUCKET"]
    s3 = s3_client()

    stats: dict = {}
    resource = None
    results = None
    try:
        with psycopg.connect(pg_conninfo()) as conn:
            stats = fetch_stats(conn, d_start, d_end, min_occurrences)
            assert_not_empty(stats, filename)

            with tempfile.TemporaryDirectory(prefix="datagouv-") as tmp:
                csv_path = os.path.join(tmp, filename)
                stream_csv(conn, d_start, d_end, min_occurrences, csv_path)

                if debug:
                    description = build_description(d_start, d_end, stats)
                    results = run_checks(stats, csv_path)
                    verdict = render_markdown(results)
                    print(verdict)

                    s3_upload(bucket, debug_csv_key(month, ts), csv_path, client=s3)
                    md = description + "\n\n## Verdict de cohérence\n\n" + verdict + "\n"
                    with open(os.path.join(tmp, "desc.md"), "w") as f:
                        f.write(md)
                    s3_upload(bucket, debug_md_key(month, ts), os.path.join(tmp, "desc.md"), client=s3)
                    print(f"🐞 debug : artefacts sur {debug_csv_key(month, ts)} (pas de publication data.gouv)")
                elif os.getenv("APP_DATAGOUV_UPLOAD", "").lower() in ("1", "true", "yes", "on"):
                    client = DataGouvClient(
                        os.environ["APP_DATAGOUV_URL"],
                        os.environ["APP_DATAGOUV_KEY"],
                        os.environ["APP_DATAGOUV_DATASET"],
                    )
                    resource = client.upload(csv_path)
                    client.set_metadata(resource, build_description(d_start, d_end, stats))
                    print(f"✅ resource publiée : {resource.get('id')}")
                else:
                    print("ℹ️ upload data.gouv désactivé (APP_DATAGOUV_UPLOAD) — dry-run")

        report = build_report(
            month=month, start=d_start, end=d_end, min_occurrences=min_occurrences,
            stats=stats, filename=filename, status="success",
            started_at=started_at, finished_at=_now_iso(), resource=resource,
            mode="debug" if debug else "live",
            checks=[vars(r) for r in results] if debug else None,
        )
        write_report(s3, bucket, report_key(month, ts), report)
        print(f"✅ {filename} — {stats.get('count_exposed')} trajets exposés")

        if debug and has_failure(results):
            raise typer.Exit(code=1)
    except typer.Exit:
        # Sortie volontaire (verdict debug FAIL) : le rapport est déjà écrit ci-dessus,
        # ne pas le faire passer pour une erreur de publication dans le except générique.
        raise
    except Exception as e:
        # Détail complet côté logs k8s uniquement ; le rapport persisté sur S3 ne garde
        # qu'un message générique (une erreur de connexion PG peut porter host/port/user).
        report = build_report(
            month=month, start=d_start, end=d_end, min_occurrences=min_occurrences,
            stats=stats, filename=filename, status="failure",
            started_at=started_at, finished_at=_now_iso(),
            error="publication data.gouv échouée",
        )
        try:
            write_report(s3, bucket, report_key(month, ts), report)
        except Exception as report_err:
            print(f"⚠️ échec écriture du rapport : {report_err!r}")
        print(f"❌ publication data.gouv échouée : {e!r}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

"""On-demand export worker.

Drains the API's pending export queue: claim -> COPY to CSV -> zip -> upload to
the export bucket -> report success/failure back to the API.
"""

import os
import zipfile
from datetime import datetime

import psycopg
import typer
from dotenv import load_dotenv

from pipelines.helpers.api_client import ApiClient
from pipelines.helpers.export_query import build_copy_sql
from pipelines.helpers.pg import pg_conninfo
from pipelines.helpers.s3 import export_s3_client, s3_upload

load_dotenv()
app = typer.Typer()

TARGETS = ["operator", "territory"]


def _to_local_date(iso: str) -> str:
    # normalize ISO datetime to YYYY-MM-DD (day-boundary semantics like start_date_filter)
    return datetime.fromisoformat(iso).strftime("%Y-%m-%d")


def stream_csv(conn, inner_sql: str, csv_path: str) -> None:
    # server-side streaming COPY straight to a local file, semicolon-delimited + header
    copy_sql = f"COPY ({inner_sql}) TO STDOUT (FORMAT CSV, DELIMITER ';', HEADER)"
    with open(csv_path, "wb") as f, conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for chunk in copy:
                f.write(chunk)


def process_one(api, conn, s3, bucket) -> bool:
    task = api.claim(TARGETS)
    if not task:
        return False
    uuid = task["uuid"]
    try:
        params = dict(task["params"])
        params["start_at"] = _to_local_date(params["start_at"])
        params["end_at"] = _to_local_date(params["end_at"])
        inner = build_copy_sql(task["target"], params)

        csv_path = f"./{uuid}.csv"
        zip_path = f"./{uuid}.csv.zip"
        stream_csv(conn, inner, csv_path)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(csv_path, arcname=f"{uuid}.csv")

        s3_upload(bucket, f"{uuid}.csv.zip", zip_path, client=s3)
        api.complete(uuid, os.path.getsize(zip_path))
        return True
    except Exception as e:  # any failure -> report to API, don't crash the loop
        # Full detail stays worker-side (logs); the API/user only sees a generic
        # message — raw psycopg/botocore errors can leak schema/hostnames.
        print(f"❌ export {uuid} failed: {e!r}")
        api.fail(uuid, "export generation failed")
        return True
    finally:
        for p in (f"./{uuid}.csv", f"./{uuid}.csv.zip"):
            if os.path.exists(p):
                os.unlink(p)


@app.command()
def run(max_iterations: int = 50):
    api = ApiClient(os.environ["API_URL"],
                    os.environ["EXPORT_WORKER_ACCESS_KEY"],
                    os.environ["EXPORT_WORKER_SECRET_KEY"])
    s3 = export_s3_client()
    bucket = os.environ["EXPORT_S3_BUCKET"]
    processed = 0
    with psycopg.connect(pg_conninfo()) as conn:
        while processed < max_iterations and process_one(api, conn, s3, bucket):
            processed += 1
    print(f"✅ processed {processed} export(s)")


if __name__ == "__main__":
    app()

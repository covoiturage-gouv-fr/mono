import pandas as pd
from sqlmesh import ExecutionContext, model
from .exporter import MODEL_COLUMNS, build_and_upload_incentives_year

@model(
    "archive_zone.archive_incentives_2024",
    kind="FULL",
    columns=MODEL_COLUMNS,
    tags=["archive", "incentives"],
)
def execute(context: ExecutionContext, **kwargs):
    result = build_and_upload_incentives_year(context, 2024)
    if result:
        yield pd.DataFrame([result])
    yield from ()

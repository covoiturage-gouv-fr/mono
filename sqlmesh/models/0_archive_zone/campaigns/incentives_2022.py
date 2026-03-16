import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.incentives_export import MODEL_COLUMNS, build_and_upload_incentives_year


@model(
    "archive_zone.archive_incentives_2022",
    kind="FULL",
    columns=MODEL_COLUMNS,
    tags=["archive", "incentives"],
)
def execute(context: ExecutionContext, **kwargs):
    result = build_and_upload_incentives_year(context, 2022)
    if result:
        yield pd.DataFrame([result])
    yield from ()

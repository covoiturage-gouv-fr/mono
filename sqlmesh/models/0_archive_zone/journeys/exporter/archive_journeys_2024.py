import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.journeys_export import MODEL_COLUMNS, build_and_upload_journeys_year

@model(
    "archive_zone.archive_journeys_2024",
    kind="FULL",
    columns=MODEL_COLUMNS,
    tags=["archive", "stage-2"],
    depends_on=["archive_zone.journeys_2024"],
)
def execute(context: ExecutionContext, **kwargs):
    result = build_and_upload_journeys_year(context, 2024)
    if result:
        yield pd.DataFrame([result])
    yield from ()

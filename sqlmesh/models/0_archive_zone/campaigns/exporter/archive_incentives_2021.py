import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.incentives_export import MODEL_COLUMNS, build_and_upload_incentives_year

@model(
    "archive_zone.archive_incentives_2021",
    kind="FULL",
    columns=MODEL_COLUMNS,
    tags=["archive", "incentives"],
    depends_on=["archive_zone.campaign_incentives_2021"],
)
def execute(context: ExecutionContext, **kwargs):
    result = build_and_upload_incentives_year(context, 2021)
    if result:
        yield pd.DataFrame([result])
    yield from ()

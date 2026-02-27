# SQLMesh Data Warehouse Architecture

Python-based SQL transformation layer using SQLMesh with a 4-zone data architecture.

## Project Structure

```
sqlmesh/
├── config.yaml             # SQLMesh configuration
├── pyproject.toml          # Python dependencies (UV)
├── .env.example            # Environment template
├── models/                 # Data transformation models
│   ├── 1_raw_zone/         # Raw data imports
│   ├── 2_trusted_zone/     # Cleaned & validated data
│   ├── 3_refined_zone/     # Analysis-ready aggregates
│   └── 4_archive_zone/     # Historical exports to S3
├── macros/                 # Reusable SQL/Jinja2 templates
├── utils/                  # Python utilities
├── tests/                  # Test files
└── audits/                 # Data quality assertions
```

## Commands

```bash
# Setup
cd sqlmesh
uv sync

# Development
sqlmesh plan dev                     # Analyze changes
sqlmesh plan dev --auto-apply        # Apply changes

# Query data
sqlmesh fetchdf "SELECT * FROM raw_zone__dev.model LIMIT 100"

# Production
sqlmesh plan                         # Production environment
```

## 4-Zone Architecture

| Zone | Purpose | Model Type | Naming |
|------|---------|------------|--------|
| Raw | External data imports | FULL | `raw_zone.<source>` |
| Trusted | Cleaned, enriched data | INCREMENTAL_BY_TIME_RANGE | `trusted_zone.<entity>` |
| Refined | Aggregated KPIs | INCREMENTAL_BY_TIME_RANGE | `refined_zone.<metric>_<granule>` |
| Archive | S3 exports | FULL | `archive_zone.archive_<entity>_<year>` |

### Data Flow

```
External Sources (S3, APIs, Files)
         |
    Raw Zone (Python models)
         |
    Trusted Zone (SQL transformations)
         |
    Refined Zone (Aggregations)
         |
    Archive Zone (S3 export)
```

## Configuration

### config.yaml

```yaml
gateways:
  postgres:
    type: postgres
    host: ${MESHPGHOST}
    database: ${MESHPGDBNAME}

physical_schema_mapping:
  raw_zone: raw_zone
  trusted_zone: trusted_zone
  refined_zone: refined_zone
  archive_zone: archive_zone

model_defaults:
  dialect: postgres
  start: 2025-09-03
  cron: "@daily"
```

### Environment Variables

```bash
MESHPGHOST=127.0.0.1
MESHPGPORT=5432
MESHPGUSER=postgres
MESHPGPASSWORD=...
MESHPGDBNAME=local
S3_ENDPOINT=...
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
```

## Model Patterns

### Raw Zone (Python)

```python
@model(
    "raw_zone.model_name",
    kind="FULL",
    columns=COLUMN_TYPES,
    grain=("id", "date"),
    tags=["raw", "source"]
)
def execute(context, start, end, execution_time, **kwargs):
    df = load_dataset(url, columns)
    return df
```

Data sources: S3 Parquet, HTTP APIs, CSV/Excel files, GeoPackage.

### Trusted Zone (SQL)

```sql
MODEL (
  name trusted_zone.journeys_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2024-01-01',
  grain '_id',
  tags ['trusted', 'journeys']
);

SELECT
  _id,
  {{get_timezoned_timestamp("start_geo_code", "start_datetime")}} AS start_datetime_tz,
  h3_lat_lng_to_cell(start_position, 9) AS start_h3_index,
  ...
FROM raw_zone.journeys_2024
WHERE start_datetime BETWEEN @start_ds AND @end_ds
```

### Refined Zone (SQL)

```sql
MODEL (
  name refined_zone.obs_journeys_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (time_column journey_date, lookback 1),
  grain ['start_geo_code', 'end_geo_code', 'journey_date'],
  tags ['refined', 'observatory']
);

SELECT
  journey_date,
  start_geo_code,
  end_geo_code,
  COUNT(*) AS journeys,
  SUM(passenger_seats) AS passengers
FROM trusted_zone.journeys
WHERE journey_date BETWEEN @start_ds AND @end_ds
GROUP BY 1, 2, 3
```

## Key Macros

### journeys_model_generator.sql

Core journey transformation logic:
- Geographic code evolution handling
- Timezone conversion
- H3 index generation
- Fraud/anomaly label aggregation
- Incentive calculations

### get_timezoned_timestamp.sql

France timezone conversion:

```sql
{% macro get_timezoned_timestamp(geo_code_col, datetime_col) %}
  CASE
    WHEN {{geo_code_col}} ~ '^97[1-2]' THEN ... AT TIME ZONE 'America/Guadeloupe'
    WHEN {{geo_code_col}} ~ '^973'     THEN ... AT TIME ZONE 'America/Cayenne'
    WHEN {{geo_code_col}} ~ '^974'     THEN ... AT TIME ZONE 'Indian/Reunion'
    WHEN {{geo_code_col}} ~ '^976'     THEN ... AT TIME ZONE 'Indian/Mayotte'
    ELSE                                     ... AT TIME ZONE 'Europe/Paris'
  END
{% endmacro %}
```

### get_final_acquisition_status_list.sql

Valid acquisition statuses:
```sql
('processed', 'failed', 'canceled', 'expired', 'terms_violation_error')
```

## Python Utilities

| Module | Purpose |
|--------|---------|
| `utils/loading.py` | Load CSV, Excel, Parquet, GeoPackage |
| `utils/cleaning.py` | Column normalization, type casting |
| `utils/s3.py` | S3 client initialization |
| `utils/export_data.py` | Query export to CSV/Parquet |
| `utils/upload.py` | S3 multipart upload |

## Data Sources

### PostgreSQL (Main)

```
carpool_v2.carpools          # Core journey data
carpool_v2.geo               # Geographic data
carpool_v2.status            # Acquisition status
policy.incentives            # Incentive data
operator.operators           # Operator metadata
company.companies            # Company info (SIRET)
```

### External

- **S3**: Journey parquets, geographic datasets (IGN, INSEE, CEREMA)
- **HTTP APIs**: transport.data.gouv.fr (carpool parking areas)

## Column Conventions

| Pattern | Example | Purpose |
|---------|---------|---------|
| `<entity>_datetime` | `start_datetime` | UTC timestamp |
| `<entity>_datetime_tz` | `start_datetime_tz` | Localized timestamp |
| `<dir>_geo_code` | `start_geo_code` | INSEE commune code |
| `<dir>_h3_index` | `start_h3_index` | H3 spatial index |
| `<source>_amount` | `operator_incentive_amount` | Financial amounts |
| `<process>_status` | `fraud_status` | Status fields |

## Best Practices

1. **Zone-First Design**: Plan transformations across all 4 zones
2. **Incremental Models**: Prefer `INCREMENTAL_BY_TIME_RANGE` for performance
3. **1-Day Lookback**: Handle late-arriving data
4. **Explicit Grain**: Define grain early for incremental updates
5. **Indexing**: B-tree for filtering, GiST for geometry
6. **Geographic**: Convert to EPSG:4326, store as WKT
7. **Tagging**: Use tags for model discovery

## Journey Data

6 years of data (2020-2025), partitioned by year:
- `raw_zone.journeys_2024`
- `trusted_zone.journeys_2024`
- `trusted_zone.journeys` (UNION view of all years)
- `archive_zone.archive_journeys_2024`

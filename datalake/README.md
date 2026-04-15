Generate sources yml from table

```bash
dbt run-operation generate_source --args '{"schema_name": "fraudcheck", "database_name": "local","generate_columns": true, "table_names":["labels"]}' --profiles-dir ./profiles/
```

Generate a model from sql

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["interoperators_labels_by_month"]}' --profiles-dir ./profiles/
Generate sources yml
```

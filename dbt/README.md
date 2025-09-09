Generate sources yml from existing sql table

```bash
dbt run-operation generate_source --args '{"schema_name": "fraudcheck", "database_name": "local","generate_columns": true, "table_names":["labels"]}' --profiles-dir ./profiles/
```

Run a single model 
```bash
dbt run --select interoperators_labels_by_month
```

Generate a yml model from sql

Note:  you need to run the model once before it is able to generate the associated yml file

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["interoperators_labels_by_month"]}' --profiles-dir ./profiles/
```

Generate source yml from existing table
```bash
dbt run-operation generate_source --args '{"schema_name": "anomaly", "table_names": ["labels"], "generate_columns": true}'
```

Run the acyclic graph documentation
```bash
dbt docs serve
```



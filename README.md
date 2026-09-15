# ETL Audit Framework

This project is a simple ETL auditing setup built around a PostgreSQL-based workflow. It tracks when a load starts, how many records are coming from the source, what gets deleted and inserted in the target, and whether the job finishes successfully.

The main idea is to keep a record of each execution in an audit table so you can trace what happened during a load and spot issues quickly.

## What is in this repo

- `src/psg_load.ipynb` – the PySpark notebook that reads data from the source system, clears the target table, loads the transformed records, and updates the audit log.
- `schema_change/audit_table.sql` – creates the `audit` schema and the `audit.etl_audit` table used to store execution details.
- `sample/sample_data.csv` – sample audit records showing what data is captured during a load run.
- `grafana/dashboard.json` – a basic Grafana dashboard for monitoring Spark worker health and resource usage.

## Audit tracking

The audit table stores details like:

- job and task name
- start/end timestamps
- source and target system, database, and table names
- record counts from the source
- number of rows inserted, deleted, and updated
- load status (`S`, `P`, `C`, `F`)
- error details when applicable

This gives you a clean record of each ETL run without having to dig through logs manually.

## How the load process works

1. A job ID is created for each run.
2. A record is inserted into `audit.etl_audit` to mark the job as in progress.
3. The source table is read from PostgreSQL using Spark.
4. Source row count is captured and written back to the audit table.
5. The target table is truncated.
6. Source data is transformed and written to the target layer.
7. Inserted row count and final status are updated when the job completes.

The notebook is a practical example of this pattern and is designed to be easy to follow and modify.

## Project setup

This repo assumes:

- PostgreSQL running with source and target schemas/tables
- Apache Spark available for the notebook
- JDBC connectivity configured for PostgreSQL
- Grafana set up to use the dashboard file if you want monitoring visuals

## Example usage

Run the notebook in `src/psg_load.ipynb` against your PostgreSQL environment. Once the job finishes, you can look at `audit.etl_audit` to review the execution history.

A typical query would be:

```sql
SELECT *
FROM audit.etl_audit
ORDER BY start_time DESC;
```

This makes it easy to see which loads succeeded, how much data moved, and where a process may have failed.

## Notes

This is a lightweight example framework for ETL auditing, not a full enterprise-grade orchestration platform. It works well as a starting point for tracking table loads and understanding job health during data movement.

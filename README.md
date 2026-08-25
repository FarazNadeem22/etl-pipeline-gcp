# ETL Pipeline — Daily Price Ingestion (GCP / Cloud Composer)

## Business problem
Ad-hoc, manually-run data pulls don't scale and don't recover cleanly from failure. This project builds a small but *correctly designed* scheduled pipeline — daily price data extracted, validated, and loaded into BigQuery — with the property that matters most in production pipelines: **it can be safely rerun without corrupting data**, which is the difference between a pipeline that fails loudly and recovers, and one that silently double-counts.

## Approach
1. **Extract** — pull daily price data from a public API/source.
2. **Load (staging)** — land it in a BigQuery staging table, partitioned by ingestion date.
3. **Data quality check** — fail loudly if row count is 0 or a key column has unexpected nulls, before anything downstream runs.
4. **Transform (idempotent)** — `MERGE` the staged data into a clean, deduplicated target table — an upsert, not a blind append, so a retry after a partial failure never creates duplicate rows.
5. **Orchestration** — an Airflow/Cloud Composer DAG scheduling and sequencing all of the above, `@daily`.

## Why this design
This is deliberately narrow in scope (see the course's Week 8 notes) — it's a "don't get disqualified by a data-engineering question" project, not a full data-engineering platform. The one property it insists on getting right is **idempotency**, because that's the property that separates a pipeline someone can trust from one that quietly corrupts data on the first retry.

## Repo structure
```
dags/
  daily_price_ingestion_dag.py   -- Airflow/Cloud Composer DAG definition
sql/
  merge_upsert.sql                -- the idempotent MERGE statement, standalone and documented
requirements.txt
```

## Running it
This is designed to run under Airflow / Cloud Composer, not as a standalone script:
```bash
pip install -r requirements.txt
# Point AIRFLOW_HOME at a local Airflow install, then:
airflow dags test daily_price_ingestion 2026-08-25
```
`sql/merge_upsert.sql` can also be run directly in the BigQuery console once staging/target tables exist, to see the idempotent-upsert logic on its own.

## So what
*(Fill in once running against a real GCP project: what the pipeline ingests, how often, and what happens — concretely — if a run is retried after a partial failure, demonstrated rather than just claimed.)*

## Honest scope notes
- Scoped to one small pipeline demonstrating correct design, not a general-purpose ingestion framework.
- ETL/ELT choice, idempotency approach, and data-quality checks are all stated explicitly in code comments — treat this as a design-judgment demonstration as much as a working pipeline.

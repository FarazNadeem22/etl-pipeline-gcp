"""
Daily price ingestion DAG: extract -> stage in BigQuery -> data-quality check
-> idempotent MERGE into the clean target table.

Design notes (the things an interviewer would ask about):
- catchup=False: a fresh deploy of this DAG should NOT try to backfill every
  day since start_date — that's a common footgun. Backfills should be a
  deliberate, explicit action, not an accident of deployment timing.
- The DAG fails loudly (raises) on a data-quality violation rather than
  silently loading bad/incomplete data downstream.
- The load step uses MERGE (see sql/merge_upsert.sql), not INSERT, so a
  retried run after a partial failure never creates duplicate rows for the
  same (symbol, date) — that's the idempotency property this whole project
  is built to demonstrate.
"""
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

PROJECT_ID = "your-project-id"
STAGING_TABLE = f"{PROJECT_ID}.staging.daily_prices_raw"
TARGET_TABLE = f"{PROJECT_ID}.clean.daily_prices"
MERGE_SQL_PATH = Path(__file__).parent.parent / "sql" / "merge_upsert.sql"


def extract_and_stage(**context) -> None:
    """Pull raw daily price data from the source API and load it into the
    BigQuery staging table, partitioned by ingestion date.

    Left as a stub for you to wire to a real source (an API, a public
    dataset) — the important part of this project is the pipeline design
    around it, not the specific source.
    """
    execution_date = context["ds"]
    print(f"[extract_and_stage] Would fetch prices for {execution_date} "
          f"and load to {STAGING_TABLE}")
    # from google.cloud import bigquery
    # client = bigquery.Client(project=PROJECT_ID)
    # ... fetch data, load via client.load_table_from_dataframe(...) ...


def check_data_quality(**context) -> None:
    """Fail loudly, before the transform step runs, if the staged data
    looks wrong. This is intentionally simple — the point is that even a
    minimal check beats none."""
    execution_date = context["ds"]
    row_count = 0  # replace with: a real BigQuery COUNT(*) query against STAGING_TABLE for this date
    null_key_count = 0  # replace with: a real query counting NULLs in required key columns

    if row_count == 0:
        raise AirflowFailException(
            f"No rows staged for {execution_date} — failing before transform runs."
        )
    if null_key_count > 0:
        raise AirflowFailException(
            f"{null_key_count} rows with NULL key columns staged for {execution_date}."
        )
    print(f"[check_data_quality] {row_count} rows, {null_key_count} null-key rows — OK")


def transform_and_load(**context) -> None:
    """Run the idempotent MERGE from sql/merge_upsert.sql, upserting staged
    rows into the clean target table. Safe to rerun."""
    merge_sql = MERGE_SQL_PATH.read_text()
    print(f"[transform_and_load] Would execute MERGE into {TARGET_TABLE}:\n{merge_sql[:200]}...")
    # from google.cloud import bigquery
    # client = bigquery.Client(project=PROJECT_ID)
    # client.query(merge_sql).result()


with DAG(
    dag_id="daily_price_ingestion",
    description="Extract daily prices, validate, idempotently load to BigQuery",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,  # deliberate — see module docstring
    tags=["portfolio-project", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_and_stage",
        python_callable=extract_and_stage,
    )
    quality_check = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )
    load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    extract >> quality_check >> load

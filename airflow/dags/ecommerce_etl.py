from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "althafmohd",
    "retries": 1,
}


with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="End-to-end E-commerce Data Engineering Pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=[
        "ecommerce",
        "data-engineering",
        "pyspark",
        "aws",
        "postgresql",
    ],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Transform raw CSV data using PySpark
    # ---------------------------------------------------------
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="""
        cd /Users/althafmohd/E-commerce

        export PATH="/Users/althafmohd/.local/bin:/opt/homebrew/opt/openjdk@17/bin:$PATH"

        export AIRFLOW_HOME="/Users/althafmohd/E-commerce/airflow"

        eval "$(aws configure export-credentials --format env)"

        spark-submit \
          --packages org.apache.hadoop:hadoop-aws:3.4.2 \
          src/transformation/transform.py
        """,
    )

    # ---------------------------------------------------------
    # TASK 2: Load processed data into PostgreSQL
    # ---------------------------------------------------------
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="""
        cd /Users/althafmohd/E-commerce

        export PATH="/Users/althafmohd/.local/bin:/opt/homebrew/opt/openjdk@17/bin:$PATH"

        export AIRFLOW_HOME="/Users/althafmohd/E-commerce/airflow"

        eval "$(aws configure export-credentials --format env)"

        spark-submit \
          --jars /tmp/postgresql-42.7.8.jar \
          --packages org.apache.hadoop:hadoop-aws:3.4.2 \
          src/loading/load_to_postgres.py
        """,
    )

    # ---------------------------------------------------------
    # TASK DEPENDENCY
    # ---------------------------------------------------------
    transform_data >> load_to_postgres
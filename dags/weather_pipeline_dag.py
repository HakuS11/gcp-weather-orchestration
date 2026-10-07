from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


PROJECT_ID = "weather-orchestration-lab"
REGION = "us-central1"
BUCKET_NAME = "weather-orchestration-lab-833242243860-data"

SPARK_SERVICE_ACCOUNT = (
    f"weather-spark-runner@{PROJECT_ID}.iam.gserviceaccount.com"
)

SUBNETWORK_URI = (
    f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
)


with DAG(
    dag_id="weather_pipeline",
    description="Clean weather data with Spark and summarize it in BigQuery.",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["weather", "dataproc", "bigquery"],
) as dag:

    run_spark_transformation = DataprocCreateBatchOperator(
        task_id="run_spark_transformation",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id="google_cloud_default",
        batch_id="weather-{{ dag_run.run_id | lower | replace('_', '-') | replace(':', '') | replace('+', '-') | replace('.', '-') }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": (
                    f"gs://{BUCKET_NAME}/scripts/transform_weather.py"
                ),
                "args": [
                    "--input",
                    f"gs://{BUCKET_NAME}/raw/weather/weather.ndjson",
                    "--output",
                    f"{PROJECT_ID}.weather_pipeline.weather_daily_clean",
                ],
            },
            "runtime_config": {
                "version": "2.2",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SPARK_SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNETWORK_URI,
                },
            },
        },
    )

    validate_clean_table = BigQueryInsertJobOperator(
        task_id="validate_clean_table",
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/validate_weather.sql' %}",
                "useLegacySql": False,
            },
        },
    )

    build_city_summary = BigQueryInsertJobOperator(
        task_id="build_city_summary",
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/city_summary.sql' %}",
                "useLegacySql": False,
            },
        },
    )

    (
        run_spark_transformation
        >> validate_clean_table
        >> build_city_summary
    )

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

ingesting_dag = DAG(
    dag_id="ingesting_pipeline",
    description="Extract data from the geo.irceline.be api and loads into s3",
    default_args={"owner": "Yves De Boeck"},
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 11, 20),
)


with ingesting_dag:
    ingest = BatchOperator(
        task_id="yves-ingest",
        job_name="yves-ingest",
        job_definition="yves-integrated-exercise",
        job_queue="integrated-exercise-job-queue",
        region_name="eu-west-1",
        overrides={"command": [
        "python",
        "./ingest.py",
        "-d",
        "{{ ds }}",
        "-e",
        "dev"
        ]},
    )

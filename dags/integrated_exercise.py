from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="yves-integrated-exercise-pipeline",
    description="Extract data from the geo.irceline.be api and loads into s3",
    default_args={"owner": "Yves De Boeck"},
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 11, 24),
)


with dag:
    ingest = BatchOperator(
        task_id="yves-ingest",
        job_name="yves-ingest",
        job_definition="yves-integrated-excercise-ingest",
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

    transform = BatchOperator(
        task_id="yves-transform",
        job_name="yves-transform",
        job_definition="yves-integrated-exercise-transform",
        job_queue="integrated-exercise-job-queue",
        region_name="eu-west-1",
        overrides={"command": [
        "python3",
        "./transform.py",
        "-d",
        "{{ ds }}",
        "-e",
        "dev"
        ]},
    )

ingest >> transform

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="yves-integrated-exercise-pipeline-tf",
    description="Extract data from the geo.irceline.be api and loads into s3",
    default_args={"owner": "Yves De Boeck"},
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 1, 1),
)


with dag:
    ingest = BatchOperator(
        task_id="yves-ingest",
        job_name="yves-ingest",
        job_definition="yves-integrated-exercise-ingest-tf",
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
        job_definition="yves-integrated-exercise-transform-tf",
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

    load = BatchOperator(
        task_id="yves-load",
        job_name="yves-load",
        job_definition="yves-integrated-exercise-load-tf",
        job_queue="integrated-exercise-job-queue",
        region_name="eu-west-1",
        overrides={"command": [
            "python3",
            "./load.py",
            "-d",
            "{{ ds }}",
            "-e",
            "dev"
        ]},
    )

ingest >> transform >> load

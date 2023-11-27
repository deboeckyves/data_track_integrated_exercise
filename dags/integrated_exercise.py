from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="ingesting_pipeline",
    description="Extract data from the geo.irceline.be api and loads into s3",
    default_args={"owner": "Yves De Boeck"},
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 11, 20),
)

ingest = BatchOperator(
    job_name='yves-ingest',
    job_definition='yves-integrated-exercise',
    job_queue='integrated-exercise-job-queue',
    container_overrides={"command": [
      "python",
      "./ingest.py",
      "-d",
      "2023-11-23",
      "-e",
      "dev"
    ]},
)

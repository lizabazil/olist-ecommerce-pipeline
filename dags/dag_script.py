from airflow.sdk import dag, task
import pendulum
import boto3
import time
from sql.business_questions import run_all_queries


@dag(
    schedule=None, 
    start_date=pendulum.datetime(2026, 8, 8, tz="UTC"),
    catchup=False,
    tags=["ecommerce-pipeline"]
)

def ecommerce_pipeline():
    @task
    def trigger_glue_job() -> str:
        client = boto3.client("glue", region_name="eu-north-1")
        response = client.start_job_run(JobName="olist-etl-job")
        job_run_id = response["JobRunId"]

        while True:
            status = client.get_job_run(
                JobName="olist-etl-job",
                RunId=job_run_id
            )["JobRun"]["JobRunState"]

            if status == "SUCCEEDED":
                print(f"Glue job has succeeded: {job_run_id}")
                return job_run_id
            elif status in ["FAILED", "ERROR", "TIMEOUT"]:
                raise Exception(f"Glue job has failed with status: {status}")

            time.sleep(10)

    @task
    def run_athena_queries():
        run_all_queries()

    @task
    def notify_done():
        print("Pipeline completed successfully.") 

    glue_job_id = trigger_glue_job()
    running_queries = run_athena_queries()
    done = notify_done()

    glue_job_id >> running_queries >> done 

ecommerce_pipeline()

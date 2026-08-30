from airflow.sdk import dag, task
import pendulum
import boto3
import time
from sql.business_questions import run_all_queries
from sql.athena_constants import all_athena_folder_names


@dag(
    schedule=None, 
    start_date=pendulum.datetime(2026, 8, 8, tz="UTC"),
    catchup=False,
    tags=["ecommerce-pipeline"]
)

def ecommerce_pipeline():
    @task
    def trigger_glue_job() -> str:
        """
        Runs Glue ETL job in AWS, which processes data via PySpark. 

        Returns:
            None
        """
        client = boto3.client("glue", region_name="eu-north-1")
        response = client.start_job_run(JobName="olist-etl-job")
        job_run_id = response["JobRunId"]

        while True:
            status = client.get_job_run(
                JobName="olist-etl-job",
                RunId=job_run_id
            )["JobRun"]["JobRunState"]

            if status == "SUCCEEDED":
                return job_run_id
            elif status in ["FAILED", "ERROR", "TIMEOUT"]:
                raise Exception(f"Glue job has failed with status: {status}")

            time.sleep(10)

    @task
    def delete_all_files_from_athena_results_folder():
        """
        Deletes files with previous Athena query results in order not to contain old versions of data in S3. 

        Returns:
            None 
        """
        s3 = boto3.resource("s3", region_name="eu-north-1")
        bucket_name = "ecommerce-pipeline-liza"
        bucket = s3.Bucket(bucket_name)

        for folder_name in all_athena_folder_names:
            prefix = f"athena-results/{folder_name}/"
            objects_to_delete = bucket.objects.filter(Prefix=prefix)
            
            _ = objects_to_delete.delete()

        return None
            

    @task
    def run_athena_queries():
        """
        Runs Athena queries via boto3 client and saves its results to a specified folder in S3. 
        Returns:
            None
        """
        run_all_queries()
        return None 


    glue_job_id = trigger_glue_job()
    delete_athena_last_versions_files = delete_all_files_from_athena_results_folder()
    running_queries = run_athena_queries()

    glue_job_id >> delete_athena_last_versions_files >> running_queries 

ecommerce_pipeline()

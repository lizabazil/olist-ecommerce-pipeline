import boto3
import time 


def run_and_save_query(sql_query: str, output_folder: str, database: str = "olist_processed_db"):
    """
    Executes Athena query and saves results to a specific S3 subfolder.

    Args:
        sql_query: SQL query to be executed.
        output_folder: subfolder name under athena-results/
        database: Athena database name.
    
    Returns:
        None
    """
    client = boto3.client("athena")
    response = client.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            "OutputLocation": f"s3://ecommerce-pipeline-liza/athena-results/{output_folder}"
        }
    )

    # response
    query_execution_id = response["QueryExecutionId"]
    while True:
        response_status = client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status_of_query = response_status["QueryExecution"]["Status"]
        state_of_query = status_of_query["State"]
        if state_of_query == "SUCCEEDED":
            print(f"Query to folder {output_folder} has succeeded.")
            break
        elif state_of_query in ["FAILED", "CANCELLED"]:
            print(f"Query has {state_of_query}: {status_of_query["AthenaError"]["ErrorMessage"]}")

        time.sleep(2)

    return None

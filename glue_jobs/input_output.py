from schema_constants import *
from context import spark_session



def load(table_name: str, schema, s3_path: str=f"{s_three_bucket_name}raw/"):
    """
    Loads a CSV file from S3 as PySpark DataFrame with written schema. 

    Args:
        table_name: name of the table.
        schema: PySpark StructType schema to enforce. 
        s3_path: S3 path to the CSV folder.
    Returns:
        PySpark DataFrame
    """
    return spark_session.read.schema(schema) \
    .option("header", "true").option("nullValue", "").option("emptyValue", "").csv(s3_path + f"{table_name}/")

def save(df, table_name: str):
    """
    Saves cleaned PySpark DataFrame to S3 as Parquet.

    Args:
        df: cleaned PySpark DataFrame.
        table_name: used as folder name in processed/ layer.
    Returns:
        None
    """
    df.write(mode("overwrite").parquet(f"{s_three_bucket_name}processed/{table_name}"))
    return None

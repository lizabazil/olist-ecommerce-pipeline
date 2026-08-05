from schema_constants import *
from context import spark_session
from pyspark.sql.functions import year, month, col



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
    df.write.mode("overwrite").parquet(f"{s_three_bucket_name}processed/{table_name}")
    return None

def save_orders_df_with_partition_by_month_year(orders_df, table_name: str):
    """
    Saves orders dataframe, performing particion by year and month of column 'order_purchase_timestamp'. 
    Then the data is saved to S3 as Parquet.

    Args:
        orders_df: cleaned orders PySpark DataFrame.
        table_name: used as folder name in processed/ layer.
    Returns:
        None
    """
    purchase_year_col = "purchase_year"
    purchase_month_col = "purchase_month"

    # adding to the dataframe two columns with year and month of purchase
    orders_df = orders_df.withColumn(purchase_year_col, year(col(order_purchase_timestamp_col))) \
                         .withColumn(purchase_month_col, month(col(order_purchase_timestamp_col)))
    orders_df.write.partitionBy(purchase_year_col, purchase_month_col) \
                   .mode("overwrite") \
                   .parquet(f"{s_three_bucket_name}processed/{table_name}")
    return None

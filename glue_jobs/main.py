import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from schema_constants import *
from pyspark.sql.functions import col, sum, hour, minute, second, count
import pyspark.sql.types as t
import pyspark.sql.functions as f
from cleaning import delete_duplicates, transform_column_to_timestamp_type, delete_rows_with_invalid_lat_and_long, delete_column


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def load(table_name: str, schema, s3_path: str="s3://ecommerce-pipeline-liza/raw/"):
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

def inspect_nulls(df):
    rows_number = df.count()
    print(f"Total number of rows in the df: {rows_number}")
    null_counts_df = df.select([sum((col(c).isNull() | (col(c) == "" if isinstance(df.schema[c].dataType, t.StringType) else col(c).isNull())).cast("int")).alias(c) for c in df.columns])
    null_counts_df.show()

def inspecting_int_and_double_columns(list_of_dfs):
    # inspecting only int and double columns
    for curr_df in list_of_dfs:
        num_cols = [f.name for f in curr_df.schema.fields if isinstance(f.dataType, t.IntegerType) or isinstance(f.dataType, t.DoubleType)]
        curr_df.select(num_cols).summary().show()

def are_all_timestamps_have_the_same_h_m_s(df, column_name_str):
    # order_reviews 
    df.select(count(hour(f.col(column_name_str)) != 0).alias("non_zero_hours"),
              count(minute(f.col(column_name_str)) != 0).alias("non_zero_minutes"),
              count(second(f.col(column_name_str)) != 0).alias("non_zero_seconds")
              ).show()
    df.where((hour(f.col(column_name_str)) != 0) | (minute(f.col(column_name_str)) != 0) | (second(f.col(column_name_str)) != 0)).show()

def detect_timestamp_pattern_in_string_column(df, column_name):
    detected_timestamp_df = df.where(f.to_timestamp(f.col(column_name), "yyyy-MM-dd HH:mm:ss").isNotNull())
    print(f"Dataframe with rows, where column {column_name} has timestamps data, which indicates invalid data (lenght={detected_timestamp_df.count()})")
    detected_timestamp_df.show()


if __name__ == "__main__":
    customers_df = load(customers_table, customers_schema)
    geolocation_df = load(geolocation_table, geolocation_schema)
    order_items_df = load(order_items_table, order_items_schema)
    order_payments_df = load(order_payments_table, order_payments_schema)
    order_reviews_df = load(order_reviews_table, order_reviews_schema)
    orders_df = load(orders_table, orders_schema)
    product_category_name_translation_df = load(product_category_name_translation_table, product_category_name_translation_schema)
    products_df = load(products_table, products_schema)
    sellers_df = load(sellers_table, sellers_schema)

    # inspect_nulls(customers_df)
    # inspect_nulls(geolocation_df)
    # inspect_nulls(order_items_df)
    # inspect_nulls(order_payments_df)
    # inspect_nulls(order_reviews_df)
    # inspect_nulls(orders_df)
    # inspect_nulls(product_category_name_translation_df)
    # inspect_nulls(products_df)
    # inspect_nulls(sellers_df)

    customers_df = delete_duplicates(customers_df)
    order_items_df = delete_duplicates(order_items_df)
    order_payments_df = delete_duplicates(order_payments_df)
    order_reviews_df = delete_duplicates(order_reviews_df)
    orders_df = delete_duplicates(orders_df)
    products_df = delete_duplicates(products_df)


    # dealing with date columns (transfrorming them from string to timestamps type using patterns for timestamp parsing)
    order_items_df = transform_column_to_timestamp_type(order_items_df, "shipping_limit_date")

    order_reviews_df = transform_column_to_timestamp_type(order_reviews_df, "review_creation_date")
    order_reviews_df = transform_column_to_timestamp_type(order_reviews_df, "review_answer_timestamp")
    orders_df = transform_column_to_timestamp_type(orders_df, "order_purchase_timestamp")
    orders_df = transform_column_to_timestamp_type(orders_df, "order_approved_at")
    orders_df = transform_column_to_timestamp_type(orders_df, "order_delivered_carrier_date")
    orders_df = transform_column_to_timestamp_type(orders_df, "order_delivered_customer_date")
    orders_df = transform_column_to_timestamp_type(orders_df, "order_estimated_delivery_date")

    # geolocation dataframe
    geolocation_df = delete_rows_with_invalid_lat_and_long(geolocation_df)

    # order_reviews
    order_reviews_df = delete_column(order_reviews_df, "review_comment_title")  # over 70% of empty values in this column
    detect_timestamp_pattern_in_string_column(order_reviews_df, "review_comment_message")
    job.commit()
        
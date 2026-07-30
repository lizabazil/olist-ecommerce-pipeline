import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from schema_constants import *
from pyspark.sql.functions import col, sum
import pyspark.sql.types as t
import pyspark.sql.functions as f


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

print("customers df:")
print(customers_df.describe().show())

print("geolocation_df:")
print(geolocation_df.describe().show())

print("order_items_df df:")
print(order_items_df.describe().show())

print("order_payments_df df:")
print(order_payments_df.describe().show())

print("order_reviews_df df:")
print(order_reviews_df.describe().show())

print("orders_df df:")
print(orders_df.describe().show())

print("products_df df:")
print(products_df.describe().show())


job.commit()
    
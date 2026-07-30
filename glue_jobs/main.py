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
from cleaning import delete_duplicates


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

customers_before_deleting_duplicates = customers_df.count()
customers_df = delete_duplicates(customers_df)
customers_after_deleting_duplicates = customers_df.count()
print(f"customers_df df, deleted duplicates={customers_before_deleting_duplicates - customers_after_deleting_duplicates}")
print(customers_df.describe().show())

df_before = geolocation_df.count()
geolocation_df = delete_duplicates(geolocation_df)
df_after = geolocation_df.count()
print(f"geolocation_df df, deleted duplicates={df_before - df_after}")
geolocation_df.describe().show()

df_before = order_items_df.count()
order_items_df = delete_duplicates(order_items_df)
df_after = order_items_df.count()
print(f"order_items_df df, deleted duplicates={df_before - df_after}")
order_items_df.describe().show()

df_before = order_payments_df.count()
order_payments_df = delete_duplicates(order_payments_df)
df_after = order_payments_df.count()
print(f"order_payments_df df, deleted duplicates={df_before - df_after}")
order_payments_df.describe().show()

df_before = order_reviews_df.count()
order_reviews_df = delete_duplicates(order_reviews_df)
df_after = order_reviews_df.count()
print(f"order_reviews_df df, deleted duplicates={df_before - df_after}")
order_reviews_df.describe().show()

df_before = orders_df.count()
orders_df = delete_duplicates(orders_df)
df_after = orders_df.count()
print(f"orders_df df, deleted duplicates={df_before - df_after}")
orders_df.describe().show()

df_before = products_df.count()
products_df = delete_duplicates(products_df)
df_after = products_df.count()
print(f"products_df df, deleted duplicates={df_before - df_after}")
products_df.describe().show()


# dealing with date columns (transfrorming them from string to date type using patterns for timestamp parsing)
order_items_df = order_items_df.withColumn("shipping_limit_date", f.when(f.col("shipping_limit_date").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss")))
order_items_df.select("shipping_limit_date").show()
order_items_df.printSchema()

order_reviews_df = order_reviews_df.withColumn("review_creation_date", f.when(f.col("review_creation_date").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")))
order_reviews_df.select("review_creation_date").show()

order_reviews_df = order_reviews_df.withColumn("review_answer_timestamp", f.when(f.col("review_answer_timestamp").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss")))
order_reviews_df.select("review_answer_timestamp").show()

order_reviews_df.printSchema()

orders_df = orders_df.withColumn("order_purchase_timestamp", f.when(f.col("order_purchase_timestamp").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")))
orders_df = orders_df.withColumn("order_approved_at", f.when(f.col("order_approved_at").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("order_approved_at"), "yyyy-MM-dd HH:mm:ss")))
orders_df = orders_df.withColumn("order_delivered_carrier_date", f.when(f.col("order_delivered_carrier_date").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss")))
orders_df = orders_df.withColumn("order_delivered_customer_date", f.when(f.col("order_delivered_customer_date").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")))
orders_df = orders_df.withColumn("order_estimated_delivery_date", f.when(f.col("order_estimated_delivery_date").isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss")))
orders_df.select("order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", 
                 "order_estimated_delivery_date").show(10)
orders_df.printSchema()


job.commit()
    
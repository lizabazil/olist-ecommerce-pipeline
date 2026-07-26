import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from schema_constants import *
from pyspark.sql.functions import col, sum

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
    #return glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name).toDF()
    return spark_session.read.schema(schema).option("header", "true").option("nullValue", "").option("emptyValue", "").csv(s3_path + f"{table_name}/")

def inspect_nulls(df: PySparkDataFrame):
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    null_counts.show()


customers_df = load(customers_table, customers_schema)

geolocation_df = load(geolocation_table, geolocation_schema)
order_items_df = load(order_items_table, order_items_schema)
order_payments_df = load(order_payments_table, order_payments_schema)
order_reviews_df = load(order_reviews_table, order_reviews_schema)
orders_df = load(orders_table, orders_schema)
product_category_name_translation_df = load(product_category_name_translation_table, product_category_name_translation_schema)
products_df = load(products_table, products_schema)
sellers_df = load(sellers_table, sellers_schema)

inspect_nulls(customers_df)
# customers_df.show(5)
# geolocation_df.show(5)
# order_items_df.show(5)
# order_payments_df.show(5)
# order_reviews_df.show(5)
# orders_df.show(5)
# product_category_name_translation_df.show(5)
# products_df.show(5)
# sellers_df.show(5)


# # those two dataframes have invalid column names, the real names are in the first row
# orders_df = fix_header(orders_df)
# product_category_name_translation_df = fix_header(product_category_name_translation_df)


# print(f"Schema of the 'customers' df:")
# # customers_df.printSchema()
# # customers_df.show()

# print(f"Schema of the 'geolocation' df:")
# # geolocation_df.printSchema()
# # geolocation_df.show()

# print(f"Schema of the 'order_items' df:")
# # order_items_df.printSchema()
# # order_items_df.show()
# print(f"Schema of the 'order_payments' df:")
# # order_payments_df.printSchema()
# # order_payments_df.show()

# print(f"Schema of the 'order_reviews' df:")
# # order_reviews_df.printSchema()
# # order_reviews_df.show()

# print(f"Schema of the 'orders' df:")
# orders_df.printSchema()
# orders_df.show()

# print(f"Schema of the 'product_category_name_translation' df:")
# product_category_name_translation_df.printSchema()
# product_category_name_translation_df.show()

# print(f"Schema of the 'products' df:")
# products_df.printSchema()
# products_df.show()

# print(f"Schema of the 'sellers' df:")
# sellers_df.printSchema()
# sellers_df.show()


job.commit()
    
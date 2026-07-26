import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from schema_constants import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def load(table_name: str, schema, s3_path: str):
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
    return spark_session.read.schema(schema).option("header", "true").option("nullValue", "null").csv(s3_path)

def fix_header(df):
    """
    Sets proper header to a PySpark DataFrame when the columns' names are in the first row, not the header.

    Args:
        df: PySpark DataFrame where first row contains real column names.
    Returns:
        PySpark DataFrame: with correct column names
    """
    real_header = [str(val) for val in df.first()]  # get names of columns (which located in the first row, not header)
    # drop first row (with real column names) and apply real names
    new_df = df.filter(df[df.columns[0]] != real_header[0]).toDF(*real_header)
    return new_df
    

customers_df = load(customers_table, customers_schema, "s3://ecommerce-pipeline-liza/raw/customers/")
customers_df.show(10)
# geolocation_df = load(geolocation_table)
# order_items_df = load(order_items_table)
# order_payments_df = load(order_payments_table)
# order_reviews_df = load(order_reviews_table)
# orders_df = load(orders_table)
# product_category_name_translation_df = load(product_category_name_translation_table)
# products_df = load(products_table)
# sellers_df = load(sellers_table)


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
    
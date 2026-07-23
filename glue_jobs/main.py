import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from glue_jobs.schema_constants import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def load(table):
    return glueContext.create_dynamic_frame.from_catalog(database="ecommerce_raw_db", table_name=table).toDF()
    
customers_df = load(customers_table)
geolocation_df = load(geolocation_table)
order_items_df = load(order_items_table)
order_payments_df = load(order_payments_table)
order_reviews_df = load(order_reviews_table)
orders_df = load(orders_table)
product_category_name_translation_df = load(product_category_name_translation_table)
products_df = load(products_table)
sellers_df = load(sellers_table)


print(f"Schema of the 'customers' df:")
customers_df.printSchema()
print(f"Schema of the 'geolocation' df:")
geolocation_df.printSchema()

print(f"Schema of the 'order_items' df:")
order_items_df.printSchema()
print(f"Schema of the 'order_payments' df:")
order_payments_df.printSchema()

print(f"Schema of the 'order_reviews' df:")
order_reviews_df.printSchema()
print(f"Schema of the 'orders' df:")
orders_df.printSchema()

print(f"Schema of the 'product_category_name_translation' df:")
product_category_name_translation_df.printSchema()

print(f"Schema of the 'products' df:")
products_df.printSchema()

print(f"Schema of the 'sellers' df:")
sellers_df.printSchema()


job.commit()
    

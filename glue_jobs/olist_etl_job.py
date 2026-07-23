import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

def load(table):
    return glueContext.create_dynamic_frame.from_catalog(database="ecommerce_raw_db", table_name=table).toDF()
    
customers_df = load("customers")
geolocation_df = load("geolocation")
order_items_df = load("order_items")
order_payments_df = load("order_payments")
order_reviews_df = load("order_reviews")
orders_df = load("orders")
product_category_name_translation_df = load("product_category_name_translation")
products_df = load("products")
sellers_df = load("sellers")


print(f"Schema of the 'customers' table:")
customers_df.printSchema()
print(f"Schema of the 'order_items' table:")
order_items_df.printSchema()
print(f"Schema of the 'order_reviews' table:")
order_reviews_df.printSchema()
print(f"Schema of the 'orders' table:")
orders_df.printSchema()



job.commit()
    

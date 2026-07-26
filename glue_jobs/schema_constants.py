import pyspark.sql.types as t

database_name = "ecommerce_raw_db"

# tables' names
customers_table = "customers"
geolocation_table = "geolocation"
order_items_table = "order-items"
order_payments_table = "order-payments"
order_reviews_table = "order-reviews"
orders_table = "orders"
product_category_name_translation_table = "product-category-name-translation"
products_table = "products"
sellers_table = "sellers"

# defining schemas of dfs
customers_schema = t.StructType([
    t.StructField("customer_id", t.StringType(), False), 
    t.StructField("customer_unique_id", t.StringType(), False),
    t.StructField("customer_zip_code_prefix", t.IntegerType(), True),
    t.StructField("customer_city", t.StringType(), True),
    t.StructField("customer_state", t.StringType(), True)
])

geolocation_schema = t.StructType([
    t.StructField("geolocation_zip_code_prefix", t.StringType(), False),
    t.StructField("geolocation_lat", t.DoubleType(), True), 
    t.StructField("geolocation_lng", t.DoubleType(), True), 
    t.StructField("geolocation_city", t.StringType(), True),
    t.StructField("geolocation_state", t.StringType(), True)
])

order_items_schema = t.StructType([
    t.StructField("order_id", t.StringType(), False),
    t.StructField("order_item_id", t.IntegerType(), True),
    t.StructField("product_id", t.StringType(), True),
    t.StructField("seller_id", t.StringType(), True),
    t.StructField("shipping_limit_date", t.StringType(), True),  # actually a timestamp, work with it later
    t.StructField("price", t.DoubleType(), True),
    t.StructField("freight_value", t.DoubleType(), True)
])

order_payments_schema = t.StructType([
    t.StructField("order_id", t.StringType(), False),
    t.StructField("payment_sequential", t.IntegerType(), True),
    t.StructField("payment_type", t.StringType(), True),
    t.StructField("payment_installments", t.IntegerType(), True),
    t.StructField("payment_value", t.DoubleType(), True)
])

order_reviews_schema = t.StructType([
    t.StructField("review_id", t.StringType(), False),
    t.StructField("order_id", t.StringType(), False),
    t.StructField("review_score", t.IntegerType(), True),
    t.StructField("review_comment_title", t.StringType(), True),
    t.StructField("review_comment_message", t.StringType(), True),
    t.StructField("review_creation_date", t.StringType(), True),  # a date type, deal with this later
    t.StructField("review_answer_timestamp", t.StringType(), True),  # a date type, deal with this later
])

orders_schema = t.StructType([
    t.StructField("order_id", t.StringType(), False),
    t.StructField("customer_id", t.StringType(), False),
    t.StructField("order_status", t.StringType(), True),
    t.StructField("order_purchase_timestamp", t.StringType(), True),  # timestamp, deal with it later
    t.StructField("order_approved_at", t.StringType(), True),  # timestamp, deal with it later
    t.StructField("order_delivered_carrier_date", t.StringType(), True),  # timestamp, deal with it later
    t.StructField("order_delivered_customer_date", t.StringType(), True),  # timestamp, deal with it later
    t.StructField("order_estimated_delivery_date", t.StringType(), True),  # timestamp, deal with it later
])

product_category_name_translation_schema = t.StructType([
    t.StructField("product_category_name", t.StringType(), False),
    t.StructField("product_category_name_english", t.StringType(), False)
])

products_schema = t.StructType([
    t.StructField("product_id", t.StringType(), False),
    t.StructField("product_category_name", t.StringType(), True),
    t.StructField("product_name_lenght", t.IntegerType(), True),
    t.StructField("product_description_lenght", t.IntegerType(), True),
    t.StructField("product_photos_qty", t.IntegerType(), True),
    t.StructField("product_weight_g", t.IntegerType(), True),
    t.StructField("product_length_cm", t.IntegerType(), True),
    t.StructField("product_height_cm", t.IntegerType(), True),
    t.StructField("product_width_cm", t.IntegerType(), True)
])

sellers_schema = t.StructType([
    t.StructField("seller_id", t.StringType(), False),
    t.StructField("seller_zip_code_prefix", t.IntegerType(), True),
    t.StructField("seller_city", t.StringType(), True),
    t.StructField("seller_state", t.StringType(), True)
])

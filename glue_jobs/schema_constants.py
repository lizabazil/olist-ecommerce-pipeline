import pyspark.sql.types as t

s_three_bucket_name = "s3://ecommerce-pipeline-liza/"

database_name = "ecommerce_raw_db"
timestamp_pattern = "yyyy-MM-dd HH:mm:ss"

# table names
customers_table = "customers"
geolocation_table = "geolocation"
order_items_table = "order-items"
order_payments_table = "order-payments"
order_reviews_table = "order-reviews"
orders_table = "orders"
product_category_name_translation_table = "product-category-name-translation"
products_table = "products"
sellers_table = "sellers"

# field names
customer_id_col = "customer_id"
customer_unique_id_col = "customer_unique_id"
customer_zip_code_prefix_col = "customer_zip_code_prefix"
customer_city_col = "customer_city"
customer_state_col = "customer_state"

geolocation_zip_code_prefix_col = "geolocation_zip_code_prefix"
geolocation_lat_col = "geolocation_lat"
geolocation_lng_col = "geolocation_lng"
geolocation_city_col = "geolocation_city"
geolocation_state_col = "geolocation_state"
order_id_col = "order_id"
order_item_id_col = "order_item_id"
product_id_col = "product_id"
seller_id_col = "seller_id"
shipping_limit_date_col = "shipping_limit_date"
price_col = "price"
freight_value_col = "freight_value"

payment_sequential_col = "payment_sequential"
payment_type_col = "payment_type"
payment_installments_col = "payment_installments"
payment_value_col = "payment_value"

review_id_col = "review_id"
review_score_col = "review_score"
review_comment_title_col = "review_comment_title"
review_comment_message_col = "review_comment_message"
review_creation_date_col = "review_creation_date"
review_answer_timestamp_col = "review_answer_timestamp"

customer_id_col = "customer_id"
order_status_col = "order_status"
order_purchase_timestamp_col = "order_purchase_timestamp"
order_approved_at_col = "order_approved_at"
order_delivered_carrier_date_col = "order_delivered_carrier_date"
order_delivered_customer_date_col = "order_delivered_customer_date"
order_estimated_delivery_date_col = "order_estimated_delivery_date"

product_category_name_col = "product_category_name"
product_category_name_english_col = "product_category_name_english"
product_name_lenght_col = "product_name_lenght"
product_description_lenght_col = "product_description_lenght"
product_photos_qty_col = "product_photos_qty"
product_weight_g_col = "product_weight_g"
product_length_cm_col = "product_length_cm"
product_height_cm_col = "product_height_cm"
product_width_cm_col = "product_width_cm"

seller_id_col = "seller_id"
seller_zip_code_prefix_col = "seller_zip_code_prefix"
seller_city_col = "seller_city"
seller_state_col = "seller_state"


# defining schemas of dfs
customers_schema = t.StructType([
    t.StructField(customer_id_col, t.StringType(), False), 
    t.StructField(customer_unique_id_col, t.StringType(), False),
    t.StructField(customer_zip_code_prefix_col, t.IntegerType(), True),
    t.StructField(customer_city_col, t.StringType(), True),
    t.StructField(customer_state_col, t.StringType(), True)
])

geolocation_schema = t.StructType([
    t.StructField(geolocation_zip_code_prefix_col, t.StringType(), False),
    t.StructField(geolocation_lat_col, t.DoubleType(), True), 
    t.StructField(geolocation_lng_col, t.DoubleType(), True), 
    t.StructField(geolocation_city_col, t.StringType(), True),
    t.StructField(geolocation_state_col, t.StringType(), True)
])

order_items_schema = t.StructType([
    t.StructField(order_id_col, t.StringType(), False),
    t.StructField(order_item_id_col, t.IntegerType(), True),
    t.StructField(product_id_col, t.StringType(), True),
    t.StructField(seller_id_col, t.StringType(), True),
    t.StructField(shipping_limit_date_col, t.StringType(), True),  # actually a timestamp, work with it later
    t.StructField(price_col, t.DoubleType(), True),
    t.StructField(freight_value_col, t.DoubleType(), True)
])

order_payments_schema = t.StructType([
    t.StructField(order_id_col, t.StringType(), False),
    t.StructField(payment_sequential_col, t.IntegerType(), True),
    t.StructField(payment_type_col, t.StringType(), True),
    t.StructField(payment_installments_col, t.IntegerType(), True),
    t.StructField(payment_value_col, t.DoubleType(), True)
])

order_reviews_schema = t.StructType([
    t.StructField(review_id_col, t.StringType(), False),
    t.StructField(order_id_col, t.StringType(), False),
    t.StructField(review_score_col, t.IntegerType(), True),
    t.StructField(review_comment_title_col, t.StringType(), True),
    t.StructField(review_comment_message_col, t.StringType(), True),
    t.StructField(review_creation_date_col, t.StringType(), True),  # a date type, deal with this later
    t.StructField(review_answer_timestamp_col, t.StringType(), True),  # a date type, deal with this later
])

orders_schema = t.StructType([
    t.StructField(order_id_col, t.StringType(), False),
    t.StructField(customer_id_col, t.StringType(), False),
    t.StructField(order_status_col, t.StringType(), True),
    t.StructField(order_purchase_timestamp_col, t.StringType(), True),  # timestamp, deal with it later
    t.StructField(order_approved_at_col, t.StringType(), True),  # timestamp, deal with it later
    t.StructField(order_delivered_carrier_date_col, t.StringType(), True),  # timestamp, deal with it later
    t.StructField(order_delivered_customer_date_col, t.StringType(), True),  # timestamp, deal with it later
    t.StructField(order_estimated_delivery_date_col, t.StringType(), True),  # timestamp, deal with it later
])

product_category_name_translation_schema = t.StructType([
    t.StructField(product_category_name_col, t.StringType(), False),
    t.StructField(product_category_name_english_col, t.StringType(), False)
])

products_schema = t.StructType([
    t.StructField(product_id_col, t.StringType(), False),
    t.StructField(product_category_name_col, t.StringType(), True),
    t.StructField(product_name_lenght_col, t.IntegerType(), True),
    t.StructField(product_description_lenght_col, t.IntegerType(), True),
    t.StructField(product_photos_qty_col, t.IntegerType(), True),
    t.StructField(product_weight_g_col, t.IntegerType(), True),
    t.StructField(product_length_cm_col, t.IntegerType(), True),
    t.StructField(product_height_cm_col, t.IntegerType(), True),
    t.StructField(product_width_cm_col, t.IntegerType(), True)
])

sellers_schema = t.StructType([
    t.StructField(seller_id_col, t.StringType(), False),
    t.StructField(seller_zip_code_prefix_col, t.IntegerType(), True),
    t.StructField(seller_city_col, t.StringType(), True),
    t.StructField(seller_state_col, t.StringType(), True)
])

from context import job
from schema_constants import *
from pyspark.sql.functions import col, sum, hour, minute, second, count
import pyspark.sql.types as t
import pyspark.sql.functions as f
from cleaning import (delete_duplicates, transform_column_to_timestamp_type, delete_rows_with_invalid_lat_and_long, delete_column, 
                      replace_column_value_to_null, delete_rows_where_review_id_invalid, delete_rows_where_column_value_is_timestamp_instead_of_string,
                      delete_rows_where_column_value_is_null)
from input_output import load, save



def inspect_nulls(df):
    rows_number = df.count()
    print(f"Total number of rows in the df: {rows_number}")
    null_counts_df = df.select([sum((col(c).isNull() | (col(c) == "" if isinstance(df.schema[c].dataType, t.StringType) else col(c).isNull())).cast("int")).alias(c) for c in df.columns])
    null_counts_df.show()
    return None

def inspecting_int_and_double_columns(list_of_dfs):
    # inspecting only int and double columns
    for curr_df in list_of_dfs:
        num_cols = [f.name for f in curr_df.schema.fields if isinstance(f.dataType, t.IntegerType) or isinstance(f.dataType, t.DoubleType)]
        curr_df.select(num_cols).summary().show()
    return None

def are_all_timestamps_have_the_same_h_m_s(df, column_name_str):
    # order_reviews 
    df.select(count(hour(f.col(column_name_str)) != 0).alias("non_zero_hours"),
              count(minute(f.col(column_name_str)) != 0).alias("non_zero_minutes"),
              count(second(f.col(column_name_str)) != 0).alias("non_zero_seconds")
              ).show()
    df.where((hour(f.col(column_name_str)) != 0) | (minute(f.col(column_name_str)) != 0) | (second(f.col(column_name_str)) != 0)).show()
    return None

def detect_timestamp_pattern_in_string_column(df, column_name):
    detected_timestamp_df = df.where(f.to_timestamp(f.col(column_name), "yyyy-MM-dd HH:mm:ss").isNotNull())
    print(f"Dataframe with rows, where column {column_name} has timestamps data, which indicates invalid data (length={detected_timestamp_df.count()})")
    detected_timestamp_df.show()
    return None

def detect_invalid_review_id(df, column_name):
    """
    Detects rows, where the given column has invalid values. The check is performed using regex and detecting if value
    has space or digits. The check is failed if value contains space or does not contain any digits (0-9).
    """
    detected_invalid_id_df = df.where((f.col(column_name).contains(" ")) | ~(f.col(column_name).rlike(".*[0-9].*")))
    count_of_invalid_rows = detected_invalid_id_df.count()
    if count_of_invalid_rows > 0:
        print(f"Detected rows where column '{column_name}' has invalid id (length={count_of_invalid_rows}): ")
        detected_invalid_id_df.show()
    return None

def detect_multiple_cols_have_null_value(df, column_names_tuple):
    col_one, col_two, col_three, col_four = column_names_tuple
    detected_df = df.where((f.col(col_one).isNull()) & (f.col(col_two).isNull()) & (f.col(col_three).isNull()) & (f.col(col_four).isNull()))
    print(f"Detected rows where columns {column_names_tuple} are all NULL, length={detected_df.count()}")
    detected_df.show()
    return None 



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
    order_items_df = transform_column_to_timestamp_type(order_items_df, shipping_limit_date_col)

    order_reviews_df = transform_column_to_timestamp_type(order_reviews_df, review_creation_date_col)
    order_reviews_df = transform_column_to_timestamp_type(order_reviews_df, review_answer_timestamp_col)
    orders_df = transform_column_to_timestamp_type(orders_df, order_purchase_timestamp_col)
    orders_df = transform_column_to_timestamp_type(orders_df, order_approved_at_col)
    orders_df = transform_column_to_timestamp_type(orders_df, order_delivered_carrier_date_col)
    orders_df = transform_column_to_timestamp_type(orders_df, order_delivered_customer_date_col)
    orders_df = transform_column_to_timestamp_type(orders_df, order_estimated_delivery_date_col)

    # geolocation dataframe
    geolocation_df = delete_rows_with_invalid_lat_and_long(geolocation_df)

    # order_reviews
    order_reviews_df = delete_column(order_reviews_df, review_comment_title_col)  # over 70% of empty values in this column
    order_reviews_df = replace_column_value_to_null(order_reviews_df, review_comment_message_col)

    order_reviews_df = delete_rows_where_review_id_invalid(order_reviews_df, review_id_col)
    order_reviews_df = delete_rows_where_column_value_is_timestamp_instead_of_string(order_reviews_df, order_id_col)
    order_reviews_df = delete_rows_where_column_value_is_null(order_reviews_df, order_id_col)

    # saving cleaned dataframes to the s3 in parquet format
    save(customers_df, customers_table)
    save(geolocation_df, geolocation_table)
    save(order_items_df, order_items_table)
    save(order_payments_df, order_payments_table)
    save(order_reviews_df, order_reviews_table)
    save(orders_df, orders_table)
    save(product_category_name_translation_df, product_category_name_translation_table)
    save(products_df, products_table)
    save(sellers_df, sellers_table)

    job.commit()
        
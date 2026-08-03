import pyspark.sql.functions as f


def delete_duplicates(df):
    """
    Deletes duplicates in the dataframe.
    Args:
        df: PySpark DataFrame.
    Returns:
        df: PySpark DataFrame without duplicates.
    """

    return df.dropDuplicates()


def transform_column_to_timestamp_type(df, column_name):
    """
    Transforms specified column to a timestamp type in pattern: yyyy-MM-dd HH:mm:ss . 
    Args:
        df: DataFrame to be changed.
    
    Returns:
        PySpark DataFrame: Dataframe with specified column changed to a timestamp type. 
    """
    df = df.withColumn(column_name, f.when(f.col(column_name).isin("0", None), None)
                                           .otherwise(f.to_timestamp(f.col(column_name), "yyyy-MM-dd HH:mm:ss")))
    return df


def delete_rows_with_invalid_lat_and_long(geolocation_df):
    """
    In the dataframe 'geolocation' exist invalid values for columns 'geolocation_lat' and 'geolocation_lng'. 
    This function filters rows and leaves only those, who have valid values 
    (for Brazil: Latitude from -33.7508 to 5.2744; Longitude from -73.9833 to -34.7914).

    Args:
        geolocation_df: 'geolocation' dataframe.
    Rerurns:
        Dataframe: with only valid values for latitude and longtitude.
    """
    geolocation_df = geolocation_df.where(f.col("geolocation_lat").between(-33.7508, 5.2744))
    geolocation_df = geolocation_df.where(f.col("geolocation_lng").between(-73.9833, -34.7914))
    return geolocation_df 


def delete_column(df, column_name):
    """
    Drops specified column in a given dataframe.
    Args:
        df: PySpark Dataframe.
        column_name: Name of a column to be deleted (string).
    
    Returns:
        DataFrame: without deleted column.
    """
    df = df.drop(f.col(column_name))
    return df 

def replace_column_value_to_null(df, column_name):
    """
    Replaces column value to NULL on the condition that the value in the column is in the timestamp format. This function primarly made for the 
    dataframe 'order_reviews', where in the column with type string found timestamps.
    Args:
        df: DataFrame.
        column_name: 
    Returns:
        DataFrame: where values in the specified column are replaced by NULL on condition.

    """
    df = df.withColumn(column_name, f.when(f.to_timestamp(f.col(column_name), "yyyy-MM-dd HH:mm:ss").isNotNull(), None).otherwise(df[column_name]))
    return df

# for order_reviews_df dataframe
def delete_rows_where_review_id_invalid(df, column_name):
    """"
    Removes rows where value in the column invalid. For example, there is a text. 
    The condition is that rows with spaces or no digits in the column are deleted.
    The condition is that rows in the resulting dataframe must have no spaces in the column and contain at least one digit.
    Args:
        df: DataFrame order_reviews.
        columm_name: Column for performing filtering.
    """
    filtered_df = df.where(~(f.col(column_name).contains(" ")) & (f.col(column_name).rlike(".*[0-9].*")))
    return filtered_df


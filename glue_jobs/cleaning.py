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


def tranform_column_to_timestamp_type(df, column_name):
    pass
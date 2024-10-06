import pyspark.sql.functions as F
import chispa
from pyspark.sql import SparkSession

def test_for_non_logical_values(df, column:str, condition:str):
    """
    :param df: Spark dataframe
    :param column: Column upon test will be performed ex. sales
    :param condition: Condition as a string ex. 'sales < 0'
    :return: True indicating condition was satisfied, hence specifying an error
    """
    check = (df.select(column).filter(condition).count() > 0)
    return check

def test_for_duplicate_entries(df, identity_columns:list):
    """
    Check if duplicate entries exist within the dataframe
    :param df: Spark dataframe
    :param identity_columns: Identity columns that specify a unique row of data. Should be a list ex. ['id','country']
    :return: True indicating condition was satisfied, hence specifying an error
    """
    check = (df.groupBy(*identity_columns).agg(F.count("*").alias("count")).filter(F.col("count")>1).count() > 0)
    return check
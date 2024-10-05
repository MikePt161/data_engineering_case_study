import pyspark.sql.functions as F
import chispa
from pyspark.sql import SparkSession

def test_for_non_logical_values(df, column, condition):
    check = (df.select(column).filter(condition).count() > 0)
    return check

def test_for_duplicate_entries(df, identity_columns):
    check = (df.groupBy(*identity_columns).agg(F.count("*").alias("count")).filter(F.col("count")>1).count() > 0)
    return check
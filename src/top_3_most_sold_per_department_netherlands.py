from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark import SparkConf
import chispa
from pyspark.sql.functions import column

import importlib

import utility.test_functions
importlib.reload(utility.test_functions)
from utility.test_functions import *

def produce_data(spark_session,
    dataset_one_path: str,
    dataset_three_path: str,
    top_n: int = 3,
    ):
    """
    Produces a csv listing the top selling products per department.
    :param spark_session: Spark builder object to handle stopping outside function.
    :param dataset_one_path: Path to dataset_one.csv
    :param dataset_two_path: Path to dataset_one.csv
    :param dataset_three_path: Path to dataset_one.csv
    :param top_n: Return top_n best solod products per department (ex. top_n = 3 will return the top 3 selling products and so on).
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_three = spark_session.read.csv(dataset_three_path, header=True, inferSchema=True)

    # Match caller id department with products sold within the Netherlands
    produced_data = (dataset_three.filter(F.col('country') == 'Netherlands').join(dataset_one.select('id', 'area'),
                                                                                  [dataset_one.id == dataset_three.caller_id],
                                                                                  'left')
                     .drop(dataset_three.caller_id, dataset_one.id).
                     groupBy('area', 'product_sold').agg(F.sum('quantity').alias('total_products_sold')))

    # Define a window function (partition) over each department, descending by total_products_sold
    window_spec = Window.partitionBy("area").orderBy(F.col("total_products_sold").desc())

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col('rank'))

    return produced_data


def main(dataset_one_path: str = r'../data/dataset_one.csv',
         dataset_three_path: str = r'../data/dataset_three.csv',
         output_directory:str = 'top_3_most_sold_per_department_netherlands',
         top_n: int = 3,
         write_results:bool = False):
    """
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_three_path: Path pointing to dataset three
    :param output_directory: Path pointing to data saving directory
    :param top_n: Specified top_n products that will be returned per
    :param write_results: Boolean True/False, whether to write data or not
    :return: Writes csv in the output_directory if write_results is specified as True
    """

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(
            spark_session=spark,
            dataset_one_path=dataset_one_path,
            dataset_three_path=dataset_three_path,
            top_n=top_n
                                     )

        if test_for_non_logical_values(produced_data,column='total_products_sold',condition=f'total_products_sold<0'):
            raise Exception(f"{output_directory}:: Negative values were identified in the dataframe.")

        if test_for_duplicate_entries(produced_data,identity_columns=['area','product_sold']):
            raise Exception(f"{output_directory}:: Duplicate entries were identified in the dataframe.")

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

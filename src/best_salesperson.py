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
    dataset_two_path: str,
    dataset_three_path: str,
    top_n: int = 1,
    ):
    """
    Produces a csv listing the top salespersons per department.
    :param spark_session: Spark builder object to handle stopping outside function.
    :param dataset_two_path: Path to dataset_one.csv
    :param dataset_three_path: Path to dataset_one.csv
    :param top_n: Return top_n best selespersons per country (ex. top_n = 3 will return the top 3 salespersons and so on).
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)
    dataset_three = spark_session.read.csv(dataset_three_path, header=True, inferSchema=True)

    # Group by caller id and country to determine quantity sold per caller
    produced_data = dataset_three.groupBy('caller_id', 'country').agg(F.sum('quantity').alias('total_products_sold'))

    # Define a window function over each country (current partition), descending by total_products_sold
    window_spec = Window.partitionBy("country").orderBy(F.col("total_products_sold").desc())

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col('rank'))

    # Retrieve caller name and sales amount
    produced_data = produced_data.join(dataset_two.select('id', 'name', 'sales_amount'),
                                       [produced_data.caller_id == dataset_two.id],
                                       'inner').drop(dataset_two.id)

    return produced_data


def main(dataset_two_path: str = r'../data/dataset_two.csv',
         dataset_three_path: str = r'../data/dataset_three.csv',
         output_directory:str = 'best_salesperson',
         top_n: int = 1,
         write_results:bool = True):
    """
    Main function that writes data in the output directory, if checks succeed
    :param dataset_two_path: Path pointing to dataset one
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
            dataset_two_path=dataset_two_path,
            dataset_three_path=dataset_three_path,
            top_n=top_n
                                     )

        if test_for_non_logical_values(produced_data,column='sales_amount',condition=f'sales_amount<0'):
            raise Exception(f"{output_directory}:: Negative values were identified in the dataframe.")

        if test_for_duplicate_entries(produced_data,identity_columns=['caller_id','country']):
            raise Exception(f"{output_directory}:: Duplicate entries were identified in the dataframe.")

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

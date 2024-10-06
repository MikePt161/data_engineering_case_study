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
    dataset_two_path: str,
    dataset_three_path: str,
    top_n: int = 3,
    percentage_threshold: float = 75):
    """
    Produces a csv listing the top performers per department, taking account the total sales and percentage of call success.
    :param spark_session: Spark builder object to handle stopping outside function.
    :str dataset_one_path: Path to dataset_one.csv
    :str dataset_two_path: Path to dataset_one.csv
    :str dataset_three_path: Path to dataset_one.csv
    :int top_n: Return top_n candidates per department (ex. top_n = 3 will return the top 3 performers and so on).
    :float percentage_threshold: Lower threshold of successful calls that a caller should have made. Here 75 translates to 75%.
    to be considered for a bonus.
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)
    dataset_three = spark_session.read.csv(dataset_three_path, header=True, inferSchema=True)

    # To lower operation complexity, a filtering is done first to pinpoint people with high success rate.
    percentage_of_successful_calls_per_caller_id = (dataset_one.
                                                    withColumn('percentage_of_successful_calls', F.round(
        (100 * F.col('calls_successful')) / F.col('calls_made'), 2)).
                                                    filter(F.col('percentage_of_successful_calls') > percentage_threshold))

    # Produce KPI: count of products sold per candidate
    # Note: This dataframe (dataset_three) contains only data for 100 distinct callers, this is bound to create unmatched entries
    # in a future join.
    count_products_sold_per_caller_id = dataset_three.select('caller_id', 'quantity').groupBy('caller_id').agg(
        F.sum('quantity').alias('count_products_sold'))

    # Perform an inner join on pinpointed eligible bonus callers to limit rows.
    produced_data = (dataset_two.select('id', 'name', 'sales_amount').
                     join(percentage_of_successful_calls_per_caller_id,
                          [dataset_two.id == percentage_of_successful_calls_per_caller_id.id],
                          'inner').drop(percentage_of_successful_calls_per_caller_id.id))

    # Also display information about number of total products sold.
    produced_data = produced_data.join(count_products_sold_per_caller_id,
                                       [produced_data.id == count_products_sold_per_caller_id.caller_id],
                                       'left'
                                       )

    # Define a window function over each department, descending by sales amount and percentage of successful calls simultaneously.
    # This allows us to avoid possible matches and distinguish top candidates with equal matches more easily.
    window_spec = Window.partitionBy("area").orderBy(F.col("sales_amount").desc(),
                                                     F.col('percentage_of_successful_calls'))

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col('rank'))

    return produced_data


def main(dataset_one_path: str = r'../data/dataset_one.csv',
         dataset_two_path: str = r'../data/dataset_two.csv',
         dataset_three_path: str = r'../data/dataset_three.csv',
         top_n: int = 3,
         percentage_threshold: float = 75,
         output_directory:str = 'top_3',
         write_results:bool = False):

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(
            spark_session=spark,
            dataset_one_path=dataset_one_path,
            dataset_two_path=dataset_two_path,
            dataset_three_path=dataset_three_path,
            top_n=top_n,
            percentage_threshold=percentage_threshold,

                                     )

        if test_for_non_logical_values(produced_data,column='percentage_of_successful_calls',condition=f'percentage_of_successful_calls<{percentage_threshold}'):
            raise Exception(f"{output_directory}:: Negative values were identified in the dataframe.")

        if test_for_duplicate_entries(produced_data,identity_columns=['id','area']):
            raise Exception(f"{output_directory}:: Duplicate entries were identified in the dataframe.")

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

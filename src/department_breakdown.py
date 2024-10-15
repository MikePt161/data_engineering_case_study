from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkConf
import chispa
from pyspark.sql.functions import column
import logging
import logging.config

import importlib

import utility.test_functions
importlib.reload(utility.test_functions)
from utility.test_functions import *


def produce_data(dataset_one_path:str, dataset_two_path:str, spark_session):
    """
    Produces sales success rate and sales amount per department in a spark dataframe
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_two_path: Path pointing to dataset two
    :param spark_session: Spark session
    :return: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Extract total calls made and total successful calls per deparment, to infer percentage
    calls_per_department = dataset_one.groupBy('area').agg(
        F.sum('calls_made').alias('total_calls_made'),
        F.sum('calls_successful').alias('total_calls_successful')
    ).withColumn('percentage_of_successful_calls',
                 F.round((100 * F.col('total_calls_successful')) / F.col('total_calls_made'), 2))

    # Extract sales amount per department and format it accordingly
    sales_per_department = (dataset_one.select('id', 'area').join(
        dataset_two.select('id', 'sales_amount'),
        [dataset_one.id == dataset_two.id],
        'left')
        .drop('id')
        .groupBy('area')
        .agg(F.sum('sales_amount').alias('total_sales_amount'))
        .withColumn('total_sales_amount', F.format_number(F.col('total_sales_amount'), 2))
        )

    # Perform a left join to merge two dataset
    produced_data = (calls_per_department.join(sales_per_department,
                                                           [calls_per_department.area == sales_per_department.area],
                                                           'left')
                                 .drop(sales_per_department.area))

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def main(dataset_one_path:str = r'../data/dataset_one.csv',
         dataset_two_path:str = r'../data/dataset_two.csv',
         output_directory:str = 'department_breakdown',
         write_results:bool = True
         ):
    """
    :param dataset_one_path: Path to dataset one
    :param dataset_two_path: Path to dataset two
    :param output_directory: Name of data saving directory within output/
    :param write_results: Boolean True/False, whether to write data or not
    :return: Writes csv in the output_directory if write_results is specified as True
    """

    # Configure the logging system
    logging.config.fileConfig(r'../utility/logconfig.ini')

    logging.debug(f'{output_directory} : Starting log process')

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(dataset_one_path, dataset_two_path, spark_session=spark)

        logging.debug(f'{output_directory} : Successfuly extracted data')

        if test_for_non_logical_values(produced_data,column='total_sales_amount',condition='total_sales_amount<0'):
            display_message = f"{output_directory}:: Negative values were identified in the dataframe."
            logging.error(display_message)
            raise Exception(display_message)

        if test_for_duplicate_entries(produced_data,identity_columns=['area']):
            display_message = f"{output_directory}:: Duplicate entries were identified in the dataframe."
            logging.error(display_message)
            raise Exception(display_message)

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)
            logging.debug(f"{output_directory}:: Results written sucessfully.")

    except Exception as e:
            logging.error(e)

    finally:

        spark.stop()
        logging.debug(f"{output_directory}:: Spark session stopped.")

if __name__ == "__main__":
    main()

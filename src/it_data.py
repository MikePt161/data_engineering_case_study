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

def produce_data(dataset_one_path, dataset_two_path, spark_session):
    """
    Returns top 100 records, sorted by sales amount (descending) from the IT department
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_two_path: Path pointing to dataset two
    :param spark_session: Predefined spark session
    :return: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Filter data on the IT department and retrieve sales amount per caller
    produced_data = (dataset_one.select("id", "area").
               filter(F.col("area") == "IT").
               join(dataset_two.select("id", "name", "address", "sales_amount"),
                    [dataset_one.id == dataset_two.id],
                    "left"
                    )).drop(dataset_two.id).orderBy("sales_amount", ascending=[False]).limit(100)

    # spark.stop()

    return produced_data


def main(dataset_one_path = r'../data/dataset_one.csv',
         dataset_two_path = r'../data/dataset_two.csv',
         output_directory='it_data',
         write_results=False
         ):
    """
    :param dataset_one_path: Path to dataset_one.csv
    :param dataset_two_path: Path to dataset_two.csv
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

        if test_for_non_logical_values(produced_data,column='sales_amount',condition='sales_amount<0'):
            display_message = f"{output_directory}:: Negative values were identified in the dataframe."
            logging.error(display_message)
            raise Exception(display_message)

        if test_for_duplicate_entries(produced_data,identity_columns=['id']):
            display_message = f"{output_directory}:: Duplicate entries were identified in the dataframe."
            logging.error(display_message)
            raise Exception(display_message)

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)

    except Exception as e:
            logging.error(e)
            logging.debug(f"{output_directory}:: Spark session stopped.")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

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


def produce_data(dataset_one_path: str, dataset_two_path: str, spark_session):
    """
    Produces a spark dataframe that includes the address and zipcode of the people who work in the Marketing
    department.
    :param dataset_one_path: Path to dataset_one
    :param dataset_two_path: Path to dataset_two
    :param spark_session: spark session
    :return: Spark dataframe with produced data
    """

    dataset_one = spark_session.read.csv(
        dataset_one_path, header=True, inferSchema=True
    )
    dataset_two = spark_session.read.csv(
        dataset_two_path, header=True, inferSchema=True
    )

    # Specify correct regex, here 4 digits followed by 2 letters. To specify:
    # \d{4}: Four digits
    # \s?: An optional space
    # [A-Z]{2}: Two uppercase letters
    zip_code_regex = r"(\d{4}\s?[A-Z]{2})"

    # Filter data on the marketing department first, to reduce the computational complexity
    # of the join operation.
    produced_data = (
        (
            dataset_one.filter(F.col("area") == "Marketing")
            .select("id")
            .join(
                dataset_two.select("id", "address"),
                [dataset_one.id == dataset_two.id],
                "left",
            )
        )
        .drop(dataset_two.id)
        .withColumn("zip_code", F.regexp_extract(F.col("address"), zip_code_regex, 1))
    )

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def main(
    dataset_one_path: str = r"../data/dataset_one.csv",
    dataset_two_path: str = r"../data/dataset_two.csv",
    output_directory: str = "marketing_address_info",
    write_results: bool = True,
):
    """
    Main function that writes data in the output directory, if checks succeed
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_two_path: Path pointing to dataset two
    :param output_directory: Name of data saving directory within output/
    :param write_results: Boolean True/False, whether to write data or not
    :return: Writes csv in the output_directory if write_results is specified as True
    """

    # Configure the logging system
    logging.config.fileConfig(r"../utility/logconfig.ini")

    logging.debug(f"{output_directory} : Starting log process")

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(
            dataset_one_path, dataset_two_path, spark_session=spark
        )

        if test_for_duplicate_entries(produced_data, identity_columns=["id"]):
            display_message = f"{output_directory}:: Duplicate entries were identified in the dataframe."
            logging.error(display_message)
            raise Exception(display_message)

        if write_results:
            produced_data.repartition(1).write.mode("overwrite").csv(
                path=f"../output/{output_directory}", header=True
            )
            logging.debug(f"{output_directory}:: Results written sucessfully.")

    except Exception as e:
        logging.error(e)

    finally:

        spark.stop()
        logging.debug(f"{output_directory}:: Spark session stopped.")


if __name__ == "__main__":
    main()

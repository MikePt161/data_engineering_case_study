from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkConf
import chispa
from pyspark.sql.functions import column

import importlib

import utility.test_functions
importlib.reload(utility.test_functions)
from utility.test_functions import *


def produce_data(dataset_one_path, dataset_two_path, spark_session):

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)

    # Specify correct regex, here 4 digits followed by 2 letters. To specify:
    # \d{4}: Four digits
    # \s?: An optional space
    # [A-Z]{2}: Two uppercase letters
    zip_code_regex = r"(\d{4}\s?[A-Z]{2})"

    produced_data = (dataset_one.filter(F.col('area') == 'Marketing').select('id').join(
        dataset_two.select('id', 'address'),
        [dataset_one.id == dataset_two.id],
        "left"
    )).drop(dataset_two.id).withColumn("zip_code", F.regexp_extract(F.col("address"), zip_code_regex, 1))

    # spark.stop()

    return produced_data



def main(dataset_one_path = r'../data/dataset_one.csv',
         dataset_two_path = r'../data/dataset_two.csv',
         output_directory='department_breakdown',
         write_results=False
         ):

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(dataset_one_path, dataset_two_path, spark_session=spark)

        if test_for_duplicate_entries(produced_data,identity_columns=['id']):
            raise Exception(f"{output_directory}:: Duplicate entries were identified in the dataframe.")

        if write_results:
            produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)



    finally:

        spark.stop()


if __name__ == "__main__":
    main()

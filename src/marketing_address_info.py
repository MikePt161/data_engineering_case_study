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

    calls_per_department = dataset_one.groupBy('area').agg(
        F.sum('calls_made').alias('total_calls_made'),
        F.sum('calls_successful').alias('total_calls_successful')
    ).withColumn('percentage_of_successful_calls',
                 F.round((100 * F.col('total_calls_successful')) / F.col('total_calls_made'), 2))

    sales_per_department = (dataset_one.select('id', 'area').join(
        dataset_two.select('id', 'sales_amount'),
        [dataset_one.id == dataset_two.id],
        'left')
                            .drop('id')
                            .groupBy('area')
                            .agg(F.sum('sales_amount').alias('total_sales_amount'))
                            .withColumn('total_sales_amount', F.format_number(F.col('total_sales_amount'), 2))
                            )

    produced_data = (calls_per_department.join(sales_per_department,
                                                           [calls_per_department.area == sales_per_department.area],
                                                           'left')
                                 .drop(sales_per_department.area))

    return produced_data


def main(dataset_one_path = r'../data/dataset_one.csv', dataset_two_path = r'../data/dataset_two.csv',output_directory='department_breakdown'):

    spark = SparkSession.builder.getOrCreate()

    try:

        produced_data = produce_data(dataset_one_path, dataset_two_path, spark_session=spark)

        if test_for_non_logical_values(produced_data,column='total_sales_amount',condition='total_sales_amount<0'):
            raise Exception("it_data:: Negative values were identified in the dataframe.")

        if test_for_duplicate_entries(produced_data,identity_columns='area'):
            raise Exception("it_data:: Duplicate entries were identified in the dataframe.")

        produced_data.repartition(1).write.mode('overwrite').csv(path=f'../output/{output_directory}', header=True)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

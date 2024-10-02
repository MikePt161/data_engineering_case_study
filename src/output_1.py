from Tools.scripts.generate_opcode_h import header
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkConf
import chispa
from pyspark.sql.functions import column

import importlib

import utility.test_functions
importlib.reload(utility.test_functions)
from utility.test_functions import *

import os

# Set the Hadoop home directory
os.environ['HADOOP_HOME'] = r'C:\Users\user\Documents\Projects\winutils\hadoop-3.3.5'  # Make sure to use your correct path
os.environ['hadoop.home.dir'] = r'C:\Users\user\Documents\Projects\winutils\hadoop-3.3.5\bin\winutils.exe'

conf = (SparkConf().
        # set("spark.hadoop.io.nativeio.enable", "false").
        set("spark.local.dir", r"C:\Users\user\Documents\Projects\data_engineering_case_study\temp"))

def produce_it_data(dataset_one_path, dataset_two_path, spark_session):

    dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)

    it_data = (dataset_one.select("id", "area").
               filter(F.col("area") == "IT").
               join(dataset_two.select("id", "name", "address", "sales_amount"),
                    [dataset_one.id == dataset_two.id],
                    "left"
                    )).drop(dataset_two.id).orderBy("sales_amount", ascending=[False]).limit(100)

    # spark.stop()

    return it_data


def main(dataset_one_path = r'../data/dataset_one.csv', dataset_two_path = r'../data/dataset_two.csv'):

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    try:

        it_data = produce_it_data(dataset_one_path, dataset_two_path, spark_session=spark)

        if test_for_non_logical_values(it_data,column='sales_amount',condition='sales_amount<0'):
            raise Exception("it_data:: Negative values were identified in the dataframe.")

        if test_for_duplicate_entries(it_data,identity_columns='id'):
            raise Exception("it_data:: Duplicate entries were identified in the dataframe.")

        it_data.repartition(1).write.mode('overwrite').csv(path='../output/it_data', header=True)

    finally:

        spark.stop()




if __name__ == "__main__":
    main()

#
#
# " Test for duplicaates "
# (it_data.groupby('id').agg(F.count("*").alias("count")).filter(F.col("count")>1))
#
#
# (it_data.show())
# (dataset_one.select("area").distinct().show())
#
#
# dataset_one.show()
#
# dataset_two.show()
#
# dataset_three.show()
#
# dataset_two.select("name").distinct().show()
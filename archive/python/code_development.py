from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark import SparkConf
import chispa
from pyspark.sql.functions import column

"""
Code Development script
"""

dataset_one_path = r'./data/dataset_one.csv'
dataset_two_path = r'./data/dataset_two.csv'
dataset_three_path = r'./data/dataset_three.csv'


spark_session = SparkSession.builder.getOrCreate()

dataset_one = spark_session.read.csv(dataset_one_path, header=True, inferSchema=True)
dataset_two = spark_session.read.csv(dataset_two_path, header=True, inferSchema=True)
dataset_three = spark_session.read.csv(dataset_three_path, header=True, inferSchema=True)

dataset_one.show()
dataset_two.show()
dataset_three.show()
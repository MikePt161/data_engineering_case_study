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



top_n = 1
produced_data = dataset_three.groupBy('caller_id','country').agg(F.sum('quantity').alias('total_products_sold'))

# # Define a window function over each department, descending by total_products_sold
window_spec = Window.partitionBy("country").orderBy(F.col("total_products_sold").desc())
#
# # Apply row_number() to assign ranks within each partition
produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))
#
# # Filter for top_n in each partition
produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col('rank'))

# Retrieve caller name and sales amount
produced_data = produced_data.join(dataset_two.select('id','name','sales_amount'),
                   [produced_data.caller_id==dataset_two.id],
                   'inner').drop(dataset_two.id)

produced_data.show()



produced_data.show()

# top_n = 3
#
# # Match caller id department with products sold within the Netherlands
# produced_data = (dataset_three.filter(F.col('country')=='Netherlands').join(dataset_one.select('id','area'),
#                                         [dataset_one.id == dataset_three.caller_id],'left')
#                  .drop(dataset_three.caller_id,dataset_one.id).
#  groupBy('area','product_sold').agg(F.sum('quantity').alias('total_products_sold')))
#
# # Define a window function over each department, descending by total_products_sold
# # This allows us to avoid possible matches and distinguish top candidates with equal matches more easily.
# window_spec = Window.partitionBy("area").orderBy(F.col("total_products_sold").desc())
#
# # Apply row_number() to assign ranks within each partition
# produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))
#
# # Filter for top_n in each partition
# produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col('rank'))
#
# dataset_three.select('country').distinct().show()
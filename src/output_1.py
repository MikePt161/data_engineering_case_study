from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import chispa


def produce_it_data(dataset_one_path, dataset_two_path):
    spark = SparkSession.builder.getOrCreate()

    dataset_one = spark.read.csv(dataset_one_path, header=True, inferSchema=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True, inferSchema=True)

    it_data = (dataset_one.select("id", "area").
               filter(F.col("area") == "IT").
               join(dataset_two.select("id", "name", "address", "sales_amount"),
                    [dataset_one.id == dataset_two.id],
                    "left"
                    )).drop(dataset_two.id).orderBy("sales_amount", ascending=[False]).limit(100)

    return it_data


def main(dataset_one_path = r'data/dataset_one.csv', dataset_two_path = r'data/dataset_two.csv'):

    it_data = produce_it_data(dataset_one_path, dataset_two_path )




    spark.stop()



if __name__ == "__main__":
    main()

    "write df"

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
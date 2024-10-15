import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window


def produce_data_marketing_address_info(
    dataset_one_path: str, dataset_two_path: str, spark_session
):
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


def produce_data_department_breakdown(
    dataset_one_path: str, dataset_two_path: str, spark_session
):
    """
    Produces sales success rate and sales amount per department in a spark dataframe
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_two_path: Path pointing to dataset two
    :param spark_session: Spark session
    :return: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(
        dataset_one_path, header=True, inferSchema=True
    )
    dataset_two = spark_session.read.csv(
        dataset_two_path, header=True, inferSchema=True
    )

    # Extract total calls made and total successful calls per deparment, to infer percentage
    calls_per_department = (
        dataset_one.groupBy("area")
        .agg(
            F.sum("calls_made").alias("total_calls_made"),
            F.sum("calls_successful").alias("total_calls_successful"),
        )
        .withColumn(
            "percentage_of_successful_calls",
            F.round(
                (100 * F.col("total_calls_successful")) / F.col("total_calls_made"), 2
            ),
        )
    )

    # Extract sales amount per department and format it accordingly
    sales_per_department = (
        dataset_one.select("id", "area")
        .join(
            dataset_two.select("id", "sales_amount"),
            [dataset_one.id == dataset_two.id],
            "left",
        )
        .drop("id")
        .groupBy("area")
        .agg(F.sum("sales_amount").alias("total_sales_amount"))
        .withColumn(
            "total_sales_amount", F.format_number(F.col("total_sales_amount"), 2)
        )
    )

    # Perform a left join to merge two dataset
    produced_data = calls_per_department.join(
        sales_per_department,
        [calls_per_department.area == sales_per_department.area],
        "left",
    ).drop(sales_per_department.area)

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def produce_data_best_salesperson(
    spark_session,
    dataset_two_path: str,
    dataset_three_path: str,
    top_n: int = 1,
):
    """
    Produces a csv listing the top salespersons per department.
    :param spark_session: Spark builder object to handle stopping outside function.
    :param dataset_two_path: Path to dataset_one.csv
    :param dataset_three_path: Path to dataset_three.csv
    :param top_n: Return top_n best selespersons per country (ex. top_n = 3 will return the top 3 salespersons and so on).
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_two = spark_session.read.csv(
        dataset_two_path, header=True, inferSchema=True
    )
    dataset_three = spark_session.read.csv(
        dataset_three_path, header=True, inferSchema=True
    )

    # Group by caller id and country to determine quantity sold per caller
    produced_data = dataset_three.groupBy("caller_id", "country").agg(
        F.sum("quantity").alias("total_products_sold")
    )

    # Define a window function over each country (current partition), descending by total_products_sold
    window_spec = Window.partitionBy("country").orderBy(
        F.col("total_products_sold").desc()
    )

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col("rank"))

    # Retrieve caller name and sales amount
    produced_data = produced_data.join(
        dataset_two.select("id", "name", "sales_amount"),
        [produced_data.caller_id == dataset_two.id],
        "inner",
    ).drop(dataset_two.id)

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def produce_data_it_data(dataset_one_path, dataset_two_path, spark_session):
    """
    Returns top 100 records, sorted by sales amount (descending) from the IT department
    :param dataset_one_path: Path pointing to dataset one
    :param dataset_two_path: Path pointing to dataset two
    :param spark_session: Predefined spark session
    :return: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(
        dataset_one_path, header=True, inferSchema=True
    )
    dataset_two = spark_session.read.csv(
        dataset_two_path, header=True, inferSchema=True
    )

    # Filter data on the IT department and retrieve sales amount per caller
    produced_data = (
        (
            dataset_one.select("id", "area")
            .filter(F.col("area") == "IT")
            .join(
                dataset_two.select("id", "name", "address", "sales_amount"),
                [dataset_one.id == dataset_two.id],
                "left",
            )
        )
        .drop(dataset_two.id)
        .orderBy("sales_amount", ascending=[False])
        .limit(100)
    )

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def produce_data_top_3_performers(
    spark_session,
    dataset_one_path: str,
    dataset_two_path: str,
    dataset_three_path: str,
    top_n: int = 3,
    percentage_threshold: float = 75,
):
    """
    Produces a csv listing the top performers per department, taking account the total sales and percentage of call success.
    :param spark_session: Spark builder object to handle stopping outside function.
    :param dataset_one_path: Path to dataset_one.csv
    :param dataset_two_path: Path to dataset_one.csv
    :param dataset_three_path: Path to dataset_one.csv
    :param top_n: Return top_n candidates per department (ex. top_n = 3 will return the top 3 performers and so on).
    :param percentage_threshold: Lower threshold of successful calls that a caller should have made. Here 75 translates to 75%.
    to be considered for a bonus.
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(
        dataset_one_path, header=True, inferSchema=True
    )
    dataset_two = spark_session.read.csv(
        dataset_two_path, header=True, inferSchema=True
    )
    dataset_three = spark_session.read.csv(
        dataset_three_path, header=True, inferSchema=True
    )

    # To lower operation complexity, a filtering is done first to pinpoint people with high success rate.
    percentage_of_successful_calls_per_caller_id = dataset_one.withColumn(
        "percentage_of_successful_calls",
        F.round((100 * F.col("calls_successful")) / F.col("calls_made"), 2),
    ).filter(F.col("percentage_of_successful_calls") > percentage_threshold)

    # Produce KPI: count of products sold per candidate
    # Note: This dataframe (dataset_three) contains only data for 100 distinct callers, this is bound to create unmatched entries
    # in a future join.
    count_products_sold_per_caller_id = (
        dataset_three.select("caller_id", "quantity")
        .groupBy("caller_id")
        .agg(F.sum("quantity").alias("count_products_sold"))
    )

    # Perform an inner join on pinpointed eligible bonus callers to limit rows.
    produced_data = (
        dataset_two.select("id", "name", "sales_amount")
        .join(
            percentage_of_successful_calls_per_caller_id,
            [dataset_two.id == percentage_of_successful_calls_per_caller_id.id],
            "inner",
        )
        .drop(percentage_of_successful_calls_per_caller_id.id)
    )

    # Also display information about number of total products sold.
    produced_data = produced_data.join(
        count_products_sold_per_caller_id,
        [produced_data.id == count_products_sold_per_caller_id.caller_id],
        "left",
    )

    # Define a window function over each department, descending by sales amount and percentage of successful calls simultaneously.
    # This allows us to avoid possible matches and distinguish top candidates with equal matches more easily.
    window_spec = Window.partitionBy("area").orderBy(
        F.col("sales_amount").desc(), F.col("percentage_of_successful_calls")
    )

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col("rank"))

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data


def produce_data_top_3_products(
    spark_session,
    dataset_one_path: str,
    dataset_three_path: str,
    top_n: int = 3,
):
    """
    Produces a csv listing the top-selling products per department.
    :param spark_session: Spark builder object to handle stopping outside function.
    :param dataset_one_path: Path to dataset_one.csv
    :param dataset_two_path: Path to dataset_one.csv
    :param dataset_three_path: Path to dataset_one.csv
    :param top_n: Return top_n best solod products per department (ex. top_n = 3 will return the top 3 selling products and so on).
    :return: produced_data: Spark DataFrame that will be written to a csv.
    """

    dataset_one = spark_session.read.csv(
        dataset_one_path, header=True, inferSchema=True
    )
    dataset_three = spark_session.read.csv(
        dataset_three_path, header=True, inferSchema=True
    )

    # Match caller id department with products sold within the Netherlands
    produced_data = (
        dataset_three.filter(F.col("country") == "Netherlands")
        .join(
            dataset_one.select("id", "area"),
            [dataset_one.id == dataset_three.caller_id],
            "left",
        )
        .drop(dataset_three.caller_id, dataset_one.id)
        .groupBy("area", "product_sold")
        .agg(F.sum("quantity").alias("total_products_sold"))
    )

    # Define a window function (partition) over each department, descending by total_products_sold
    window_spec = Window.partitionBy("area").orderBy(
        F.col("total_products_sold").desc()
    )

    # Apply row_number() to assign ranks within each partition
    produced_data = produced_data.withColumn("rank", F.row_number().over(window_spec))

    # Filter for top_n in each partition
    produced_data = produced_data.filter(F.col("rank") <= top_n).drop(F.col("rank"))

    assert produced_data.count() > 0, "Produced Dataframe must not be empty"

    return produced_data

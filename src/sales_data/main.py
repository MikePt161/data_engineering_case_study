import argparse
from pyspark.sql import SparkSession
from sales_data.produce_data_functions import *

# Configure the logging system
# logging.config.fileConfig(r'../utility/logconfig.ini')

# logging.debug(f'Starting log process')


def main():
    parser = argparse.ArgumentParser(
        description="Process sales data. The app takes different commands which invoke the data processing functions."
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available commands"
    )

    # Subcommand for IT Data (Output #1)
    parser_it_data = subparsers.add_parser("it-data", help="Process IT department data")
    parser_it_data.add_argument("dataset_one", help="Path to dataset_one.csv")
    parser_it_data.add_argument("dataset_two", help="Path to dataset_two.csv")
    parser_it_data.add_argument("output_dir", help="Directory to save the output")

    # Subcommand for Marketing Address Info (Output #2)
    parser_marketing = subparsers.add_parser(
        "marketing-address-info", help="Process Marketing department address data"
    )
    parser_marketing.add_argument("dataset_one", help="Path to dataset_one.csv")
    parser_marketing.add_argument("dataset_two", help="Path to dataset_two.csv")
    parser_marketing.add_argument("output_dir", help="Directory to save the output")

    # Subcommand for Department Breakdown (Output #3)
    parser_department_breakdown = subparsers.add_parser(
        "department-breakdown", help="Process department breakdown data"
    )
    parser_department_breakdown.add_argument(
        "dataset_one", help="Path to dataset_one.csv"
    )
    parser_department_breakdown.add_argument(
        "dataset_two", help="Path to dataset_two.csv"
    )
    parser_department_breakdown.add_argument(
        "output_dir", help="Directory to save the output"
    )

    # Subcommand for Top 3 Performers (Output #4)
    parser_top_3_performers = subparsers.add_parser(
        "top-3-performers", help="Extract Top 3 best performers per department"
    )
    parser_top_3_performers.add_argument("dataset_one", help="Path to dataset_one.csv")
    parser_top_3_performers.add_argument("dataset_two", help="Path to dataset_two.csv")
    parser_top_3_performers.add_argument(
        "dataset_three", help="Path to dataset_three.csv"
    )
    parser_top_3_performers.add_argument(
        "output_dir", help="Directory to save the output"
    )

    # Subcommand for Top 3 sold products (Output #5)
    parser_top_3_products = subparsers.add_parser(
        "top-3-products",
        help="Top 3 most sold products per department in the Netherlands",
    )
    parser_top_3_products.add_argument("dataset_one", help="Path to dataset_one.csv")
    parser_top_3_products.add_argument("dataset_two", help="Path to dataset_two.csv")
    parser_top_3_products.add_argument(
        "dataset_three", help="Path to dataset_three.csv"
    )
    parser_top_3_products.add_argument(
        "output_dir", help="Directory to save the output"
    )

    # Subcommand for best salesperson (Output #6)
    parser_best_salesperson = subparsers.add_parser(
        "best-salesperson", help="Who is the best overall salesperson per country"
    )
    parser_best_salesperson.add_argument("dataset_one", help="Path to dataset_one.csv")
    parser_best_salesperson.add_argument(
        "dataset_three", help="Path to dataset_three.csv"
    )
    parser_best_salesperson.add_argument(
        "output_dir", help="Directory to save the output"
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    spark_session = SparkSession.builder.getOrCreate()

    try:

        if args.command == "it-data":
            write_dataframe = produce_data_it_data(
                spark_session=spark_session,
                dataset_one_path=args.dataset_one,
                dataset_two_path=args.dataset_two,
            )

        elif args.command == "marketing-address-info":
            write_dataframe = produce_data_marketing_address_info(
                spark_session=spark_session,
                dataset_one_path=args.dataset_one,
                dataset_two_path=args.dataset_two,
            )
        elif args.command == "department-breakdown":
            write_dataframe = produce_data_department_breakdown(
                spark_session=spark_session,
                dataset_one_path=args.dataset_one,
                dataset_two_path=args.dataset_two,
            )
        elif args.command == "top-3-performers":
            write_dataframe = produce_data_top_3_performers(
                spark_session=spark_session,
                dataset_one_path=args.dataset_one,
                dataset_two_path=args.dataset_two,
                dataset_three_path=args.dataset_three,
            )

        elif args.command == "top-3-products":
            write_dataframe = produce_data_top_3_products(
                spark_session=spark_session,
                dataset_one_path=args.dataset_one,
                dataset_three_path=args.dataset_three,
            )

        elif args.command == "best-salesperson":
            write_dataframe = produce_data_best_salesperson(
                spark_session=spark_session,
                dataset_two_path=args.dataset_two,
                dataset_three_path=args.dataset_three,
            )

        write_dataframe.repartition(1).write.mode("overwrite").csv(
            path=f"./output/{args.output_dir}", header=True
        )
    except Exception as e:
        raise e

    finally:

        spark_session.stop()


if __name__ == "__main__":
    main()

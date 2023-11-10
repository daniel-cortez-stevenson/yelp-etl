import logging
from argparse import ArgumentParser, Namespace
from importlib import import_module

from pyspark.sql import SparkSession

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


PIPELINE_MODULE = "yelp_etl.pipeline"


def main(known_args: Namespace, additional_args_dict: dict) -> None:
    _logger.info(
        f"Start {known_args.entity_type} processing using pipeline {known_args.pipeline}"
    )
    spark = SparkSession.builder.appName("Yelp ETL in Iceberg data lake").getOrCreate()
    # Import and run the specified pipeline function
    pipeline_module = import_module(f"{PIPELINE_MODULE}.{known_args.pipeline}")
    pipeline_function = getattr(pipeline_module, "process")
    pipeline_function(spark, known_args, **additional_args_dict)
    _logger.info(f"Completed data processing using pipeline {known_args.pipeline}")
    spark.stop()


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Data processing using Spark, MinIO and Iceberg."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Data S3 path or metastore table. E.g. 'lake.bronze.yelp.business'.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output location. E.g. 'lake.bronze.yelp.business'.",
    )
    parser.add_argument(
        "--entity_type",
        choices=["business", "review", "user", "checkin", "tip"],
        required=True,
        help="Type of entity to process. E.g. 'business'.",
    )
    parser.add_argument(
        "--pipeline",
        choices=["extract", "clean", "enrich"],
        required=True,
        help="Name of a pipeline .py module to load and run process() with.",
    )
    parser.add_argument(
        "--partition_column", help="DataFrame Column name to use for partitioning."
    )
    parser.add_argument(
        "--bucket_column", help="DataFrame Column name to use for bucketing."
    )
    parser.add_argument(
        "--buckets",
        type=int,
        help="How many buckets to create on `bucket_column` when writing.",
    )
    known_args, additional_args = parser.parse_known_args()
    # Convert additional_args from list of strings ['--arg', 'value'] to dictionary {'arg': 'value'}
    additional_args_dict = dict(zip(additional_args[::2], additional_args[1::2]))
    # Remove any leading '--' from keys in additional_args_dict
    additional_args_dict = {
        key.lstrip("--"): value for key, value in additional_args_dict.items()
    }
    main(known_args, additional_args_dict)

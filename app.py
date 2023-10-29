"""
Usage:
    $ /opt/spark/bin/spark-submit \
        # spark-submit args \
        ... \
        # This Python distribution as zipfile \
        --py-files yelp_etl.zip \
        app.py \
        ... # app.py args
"""
import argparse
import importlib
import logging

from yelp_etl.spark import setup_spark_session

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


PIPELINE_MODULE = "yelp_etl.pipeline"


def main(known_args, additional_args_dict):
    _logger.info(f"Starting data processing using pipeline {known_args.pipeline}")

    spark = setup_spark_session(known_args)

    # Import and run the specified pipeline function
    pipeline_module = importlib.import_module(
        f"{PIPELINE_MODULE}.{known_args.pipeline}"
    )
    pipeline_function = getattr(pipeline_module, known_args.pipeline)
    pipeline_function(spark, known_args, **additional_args_dict)

    _logger.info(f"Completed data processing using pipeline {known_args.pipeline}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data processing using Spark, MinIO and Iceberg."
    )
    parser.add_argument(
        "--pipeline", required=True, help="Name of the data processing pipeline to run"
    )
    parser.add_argument(
        "--buckets",
        default=8,
        type=int,
        help="How many buckets to create when writing.",
    )

    known_args, additional_args = parser.parse_known_args()
    # Convert additional_args from list of strings ['--arg', 'value'] to dictionary {'arg': 'value'}
    additional_args_dict = dict(zip(additional_args[::2], additional_args[1::2]))
    # Remove any leading '--' from keys in additional_args_dict
    additional_args_dict = {
        key.lstrip("--"): value for key, value in additional_args_dict.items()
    }

    main(known_args, additional_args_dict)

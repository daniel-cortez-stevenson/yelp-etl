from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from yelp_etl.common.write import create_iceberg_writer, create_partition_args

if TYPE_CHECKING:
    from argparse import Namespace

    from pyspark.sql import SparkSession


logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def process(
    spark: SparkSession,
    known_args: Namespace,
) -> None:
    """Convert individual Yelp JSON files to Apache Iceberg format.

    : param spark: SparkSession
    : param known_args: known_args from argparse CLI
    """
    _logger.info("Starting extract process ...")
    # Useful if 1 file per bucket
    spark.conf.set("spark.sql.legacy.bucketedTableScan.outputOrdering", True)
    df = spark.read.json(known_args.input)
    _logger.info(f"Writing an Iceberg table at {known_args.output} ...")
    df.printSchema()
    df.show()
    create_iceberg_writer(
        spark,
        df,
        known_args.output,
        partition_args=create_partition_args(
            known_args.partition_column, known_args.bucket_column, known_args.buckets
        ),
    ).createOrReplace()
    _logger.info(f"Finished writing table {known_args.output} to Iceberg format")

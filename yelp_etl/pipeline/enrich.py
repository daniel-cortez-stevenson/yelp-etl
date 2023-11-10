from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from yelp_etl.common.write import create_iceberg_writer, create_partition_args

if TYPE_CHECKING:
    from argparse import Namespace

    from pyspark.sql import SparkSession

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def process(
    spark: SparkSession,
    known_args: Namespace,
    dimension_inputs: str,
    dimension_entity_types: str,
) -> None:
    """Enrich fact tables with dimension data.
    JOINs to create "One Big Table"-style enriched fact tables.

    : param spark: SparkSession
    : param known_args: known_args from argparse CLI
    : param dimension_inputs: A comma-separated string representing a list of tables to join to the input.
    : param dimension_entity_types: A comma-separated string representing a list of dimension entity types to process.
    """
    _logger.info("Starting enrich process ...")
    # In case number of buckets is different between tables joined
    spark.conf.set("spark.sql.bucketing.coalesceBucketsInJoin.enabled", True)
    # Useful if 1 file per bucket
    spark.conf.set("spark.sql.legacy.bucketedTableScan.outputOrdering", True)
    # Join optimizations for bucketed DataFrameWriterV2-created sources
    spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled", True)
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", True)
    spark.conf.set(
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", True
    )
    df = spark.table(known_args.input)
    for dim_entity_type, dim_input in list(
        zip(dimension_entity_types.split(","), dimension_inputs.split(","))
    ):
        dim_df = spark.table(dim_input)
        # Add a column string prefix to dimensions for clarity after JOIN
        dim_df = dim_df.select(
            [F.col(c).alias(f"{dim_entity_type}_" + c) for c in dim_df.columns]
        )
        join_column = "business_id"
        if dim_entity_type == "user":
            join_column = "user_id"
        # "One Big Table"-style preemptive joins
        df = df.join(
            dim_df,
            df[join_column] == dim_df[f"{dim_entity_type}_{join_column}"],
        )
        df = df.drop(f"{dim_entity_type}_{join_column}")
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

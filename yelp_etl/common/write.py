from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

import pyspark.sql.functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameWriterV2, SparkSession


def create_iceberg_writer(
    spark: SparkSession, df: DataFrame, output: str, partition_args: List[Any] = None
) -> DataFrameWriterV2:
    """Write a DataFrame to Apache Iceberg format.

    : param df: DataFrame
    : param output: Output path
    : param partition_column: DataFrame Column name to use for partitioning.
    : param bucket_column: DataFrame Column name to use for bucketing.
    : param buckets: How many buckets to create on `bucket_column` when writing.
    """
    # TODO: Add support for multiple partition and bucket columns
    # TODO: Are the options below needed?
    writer = (
        df.writeTo(output)
        .using("iceberg")
        .option("write.object-storage.enabled", True)
        .option("write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse"))
    )
    # Bucketing reference: https://towardsdatascience.com/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53
    if partition_args:
        writer = writer.partitionedBy(*partition_args)
    return writer


def create_partition_args(
    partition_column: str = None, bucket_column: str = None, buckets: int = None
) -> List[Any]:
    """Create partition arguments for DataFrameWriterV2.partitionedBy().

    : param partition_column: DataFrame Column name to use for partitioning.
    : param bucket_column: DataFrame Column name to use for bucketing.
    : param buckets: How many buckets to create on `bucket_column` when writing.
    """
    return list(
        filter(
            lambda x: True if x is not None else False,
            [
                partition_column if partition_column else None,
                F.bucket(buckets, bucket_column) if bucket_column else None,
            ],
        )
    )

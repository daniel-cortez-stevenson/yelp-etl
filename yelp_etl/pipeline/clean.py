from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

from yelp_etl.common.write import create_iceberg_writer, create_partition_args

if TYPE_CHECKING:
    from argparse import Namespace
    from typing import List, Union

    from pyspark.sql import Column, DataFrame, SparkSession

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def process(
    spark: SparkSession,
    known_args: Namespace,
) -> None:
    """Clean up the raw Apache Iceberg data.

    # TODO: Use the strategy pattern to clean different entity types
    Check-in, Review, Tip facts:
    - Add a unique ID to checkin table
    - Explode comma separated string of timestamps in checkin table
    - Create date_week_start_date to perform business-relevant aggregations later
    - Create date_ts for sorting and activity schema analyses

    Business dimension:
    - Remove unicode syntax from strings
    - Interpret "None"-like strings as NULL
    - Unnest `hours` and `attributes` to top-level columns
    - Convert some string columns to Map<String, Boolean> or Boolean

    User dimension:
    - Create yelping_since_week_start_date to perform business-relevant aggregations later
    - Create yelping_since_ts for sorting and activity schema analyses

    : param spark: SparkSession
    : param known_args: known_args from argparse CLI
    """
    _logger.info("Starting clean process ...")
    # Date parsing will fail without this.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # Useful if 1 file per bucket
    spark.conf.set("spark.sql.legacy.bucketedTableScan.outputOrdering", True)
    df = spark.table(known_args.input)
    # Check-in-specific cleaning
    if known_args.entity_type == "checkin":
        df = df.withColumn(
            "date", F.explode(F.split("date", ", ", limit=-1))
        ).withColumn("checkin_id", F.monotonically_increasing_id())
    # Business-specific cleaning
    if known_args.entity_type == "business":
        df = df.withColumn("is_open", F.col("is_open").cast(T.BooleanType()))
        df = df.withColumn("categories", F.split("categories", ", ", limit=-1))
        start_column_names = df.columns
        # Attributes
        df = df.select(flatten_stuct_schema(df.schema, None, ["attributes"]))
        attribute_column_names = list(set(df.columns) - set(start_column_names))
        for column_name in attribute_column_names:
            # Clean the string data so that we can convert without creating NULLs
            # Clean up unicode characters and extra single quotation marks
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"^u'(.*)'$", "$1")
            )
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"u('.*?')", "$1")
            )
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"'none'", "none")
            )
            # Convert "None" or "none" to NULL
            df = df.withColumn(
                column_name,
                F.when(F.lower(F.col(column_name)) == "none", None).otherwise(
                    F.col(column_name)
                ),
            )
            # Create Valid JSON that could be cast to a Map<String, Boolean> by from_json()
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"[Nn]one", "null")
            )
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"False", "false")
            )
            df = df.withColumn(
                column_name, F.regexp_replace(column_name, r"True", "true")
            )
            df = df.withColumn(
                column_name,
                safe_convert_string(
                    df,
                    column_name,
                    [
                        T.MapType(T.StringType(), T.BooleanType()),
                        T.MapType(T.StringType(), T.StringType()),
                        T.BooleanType(),
                    ],
                ),
            )
        # Opening hours
        df = df.select(flatten_stuct_schema(df.schema, None, ["hours"]))
        hours_column_names = list(
            set(df.columns) - set(attribute_column_names) - set(start_column_names)
        )
        for column_name in hours_column_names:
            split_hours_col = F.split(df[column_name], "-")
            df = df.withColumn(f"{column_name}_start", split_hours_col.getItem(0))
            df = df.withColumn(f"{column_name}_end", split_hours_col.getItem(1))
            df = df.drop(f"{column_name}")
    # Create date features for data with a timestamp or date.
    if known_args.entity_type in ["checkin", "review", "tip", "user"]:
        # TODO: Parameterize CLI with --timestamp_column and --timestamp_format
        timestamp_format = "yyyy-MM-dd"
        if known_args.entity_type == "checkin":
            timestamp_format = "yyyy-MM-dd HH:mm:ss"
        timestamp_column = "date"
        if known_args.entity_type == "user":
            timestamp_column = "yelping_since"
        df = df.select(
            "*",
            *create_date_features(
                timestamp_column,
                timestamp_format,
            ),
        ).drop(timestamp_column)
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


def flatten_stuct_schema(
    schema: T.StructType, prefix: str = None, struct_columns: List[str] = None
) -> List[Column]:
    """Flattens a StructType schema into a list of Spark Columns.

    :param schema: DataFrame schema to flatten
    :param prefix: Prefix to add to column names
    :param struct_columns: List of columns to flatten
    """
    columns = []
    for f in schema.fields:
        column = f.name if prefix is None else f"{prefix}.{f.name}"
        if isinstance(f.dataType, T.StructType) and (
            struct_columns is None or f.name in struct_columns
        ):
            columns.extend(flatten_stuct_schema(f.dataType, column))
        else:
            columns.append(F.col(column).alias(column.replace(".", "_").lower()))
    return columns


def safe_convert_string(
    df: DataFrame,
    column_name: str,
    conversion_types: List[Union[T.DataType, T.StructType]],
) -> Column:
    """Attempts to convert a Column to one of the provided types until a valid conversion is found.
    If a type conversion creates NULLs, it is not valid.

    :param df: DataFrame which column_name belongs to
    :param column_name: name of Column in the DataFrame to convert
    :param conversion_types: list of types (complex or simple) to try to convert to in the order provided
    """
    if len(conversion_types) == 0:
        _logger.info(f"Did not convert {column_name} to any type")
        return F.col(column_name)
    target_type = conversion_types.pop(0)
    if isinstance(target_type, T.StructType) or isinstance(target_type, T.MapType):
        converted_column = F.from_json(F.col(column_name), target_type)
    else:
        converted_column = F.col(column_name).cast(target_type)
    # Count nulls in the original column and the converted column
    counts = df.select(
        F.count(F.when(F.col(column_name).isNull(), 1)).alias("max_allowed_null_count"),
        F.count(F.when(converted_column.isNull(), 1)).alias("null_count"),
    ).collect()[0]
    if counts["null_count"] <= counts["max_allowed_null_count"]:
        _logger.info(f"Converted {column_name} to {target_type}")
        return converted_column
    return safe_convert_string(df, column_name, conversion_types)


def create_date_features(
    timestamp_column_name: str,
    timestamp_format: str = None,
) -> List[Column]:
    """Create date feature Columns from a T.TimestampType Column.

    :param timestamp_column_name: A Column name to create date features from
    """
    timestamp_column = F.to_timestamp(timestamp_column_name, timestamp_format)
    return [
        timestamp_column.alias(f"{timestamp_column_name}_ts"),
        F.to_date(timestamp_column).alias(f"{timestamp_column_name}_date"),
        F.to_date(F.date_trunc("week", timestamp_column), "yyyy-MM-dd HH:mm:ss").alias(
            f"{timestamp_column_name}_week_start_date"
        ),
        F.weekofyear(timestamp_column).alias(f"{timestamp_column_name}_week"),
        F.quarter(timestamp_column).alias(f"{timestamp_column_name}_quarter"),
        F.dayofweek(timestamp_column).alias(f"{timestamp_column_name}_dayofweek"),
        F.month(timestamp_column).alias(f"{timestamp_column_name}_month"),
        F.year(timestamp_column).alias(f"{timestamp_column_name}_year"),
    ]

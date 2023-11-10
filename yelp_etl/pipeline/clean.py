from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

import pyspark.sql.functions as F
import pyspark.sql.types as T

from yelp_etl.common.write import create_iceberg_writer, create_partition_args

if TYPE_CHECKING:
    from argparse import Namespace

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
        start_columns = df.columns
        df = df.select(flatten_stuct_schema(df.schema, None, ["attributes"]))
        new_attribute_columns = list(set(df.columns) - set(start_columns))

        # Clean the string data so that we can auto-cast
        for column in new_attribute_columns:
            # Clean up unicode string values
            df = df.withColumn(column, F.regexp_replace(column, "^u'(.*)'$", "$1"))
            df = df.withColumn(column, F.regexp_replace(column, "u('.*?')", "$1"))
            # Convert "None" or "none" to NULL
            df = df.withColumn(column, F.regexp_replace(column, "'none'", "None"))
            df = df.withColumn(column, none_as_null(column))
            # Create Valid JSON that could be cast to a Map<String, Boolean>
            df = df.withColumn(column, F.regexp_replace(column, "None", "null"))
            df = df.withColumn(column, F.regexp_replace(column, "False", "false"))
            df = df.withColumn(column, F.regexp_replace(column, "True", "true"))

        df = convert_json_or_cast(
            df,
            new_attribute_columns,
            json_schema=T.MapType(T.StringType(), T.BooleanType()),
            fallback_type=T.BooleanType(),
        )
        df = df.select(flatten_stuct_schema(df.schema, None, ["hours"]))

        days_of_week = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]
        for day in days_of_week:
            split_hours_col = F.split(df[f"hours_{day}"], "-")
            df = df.withColumn(f"hours_{day}_start", split_hours_col.getItem(0))
            df = df.withColumn(f"hours_{day}_end", split_hours_col.getItem(1))
            df = df.drop(f"hours_{day}")

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


def convert_json_or_cast(
    df: DataFrame,
    columns: List[str],
    json_schema: T.StructType,
    fallback_type: T.DataType,
) -> DataFrame:
    """Converts columns to a JSON schema or a fallback type if converting the column produces NULL values

    :param df: DataFrame to transform
    :param columns: column names to convert
    :param json_schema: T.StructType representing the JSON schema of the string to convert
    :param fallback_type: fallback type to convert to if JSON conversion produces NULL values
    """
    for column in columns:
        json_column = f"{column}_json"
        df = df.withColumn(json_column, F.from_json(column, json_schema))
        df = drop_column_based_on_nulls(df, json_column, column)
        if json_column not in df.columns:
            cast_column = f"{column}_cast"
            df = df.withColumn(cast_column, F.col(column).cast(fallback_type))
            df = drop_column_based_on_nulls(df, cast_column, column)
            if cast_column in df.columns:
                df = df.drop(column)
                df = df.withColumnRenamed(cast_column, column)
                _logger.info(f"Converted {column} to {fallback_type}")
            else:
                _logger.info(f"Did not convert {column} to another type")
        else:
            df = df.drop(column)
            df = df.withColumnRenamed(json_column, column)
            _logger.info(f"Converted {column} to {json_schema}")
    return df


def drop_column_based_on_nulls(
    df: DataFrame, column: str, comparison_column: str
) -> DataFrame:
    """Drops a Column if the comparison Column has fewer NULL values.

    : param df: DataFrame to transform
    : param column: Column name to potentially drop
    : param comparison_column: Column name to use as the baseline for nulls
    """
    counts = df.select(
        F.count(F.when(F.col(column).isNull(), 1)).alias("null_count"),
        F.count(F.when(F.col(comparison_column).isNull(), 1)).alias(
            "max_allowed_null_count"
        ),
    ).collect()[0]
    if counts["null_count"] > counts["max_allowed_null_count"]:
        df = df.drop(column)
    return df


def none_as_null(column: str) -> Column:
    """Converts "None" or "none" to NULL for a DataFrame Column.

    : param column: Column name of T.StringType Column to convert
    """
    return F.when(F.lower(F.col(column)) == "none", None).otherwise(F.col(column))


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

import logging
from typing import List

from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def silver(
    spark,
    known_args,
    namespace: str,
):
    """
    Clean up the raw Apache Iceberg data.
    - Add a unique ID to checkin table
    - Explode comma separated string of timestamps in checkin table
    - Create date_week to perform business-relevant aggregations later
    - Create date_timestamp for sorting and TODO: activity schema analyses

    :param spark: SparkSession.
    :param known_args: Spark and pipeline setup args to executable.
    :param namespace: Iceberg "database" in the cluster-configured Catalog.
    """
    _logger.info("Starting silver processing...")
    # Date parsing will fail without this.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
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

    # Check-in Fact
    checkin = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.checkin"
    )
    checkin = checkin.withColumn("date", F.explode(F.split("date", ", ", limit=-1)))
    checkin = checkin.withColumn("checkin_id", F.monotonically_increasing_id())
    checkin = checkin.withColumn(
        "date_timestamp", F.to_timestamp(checkin.date, "yyyy-MM-dd HH:mm:ss")
    )
    checkin = checkin.withColumn("date", F.to_date(checkin.date, "yyyy-MM-dd HH:mm:ss"))
    checkin = checkin.withColumn(
        "date_week",
        F.to_date(F.date_trunc("week", checkin.date), "yyyy-MM-dd HH:mm:ss"),
    )

    # Review fact
    review = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.review"
    )
    review = review.withColumn(
        "date_timestamp", F.to_timestamp(review.date, "yyyy-MM-dd")
    )
    review = review.withColumn("date", F.to_date(review.date))
    review = review.withColumn(
        "date_week", F.to_date(F.date_trunc("week", review.date), "yyyy-MM-dd HH:mm:ss")
    )

    # Tip fact
    tip = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.tip"
    )
    tip = tip.withColumn("date_timestamp", F.to_timestamp(tip.date, "yyyy-MM-dd"))
    tip = tip.withColumn("date", F.to_date(tip.date))
    tip = tip.withColumn(
        "date_week", F.to_date(F.date_trunc("week", tip.date), "yyyy-MM-dd HH:mm:ss")
    )

    # Business dimension
    business = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.business"
    )
    business = business.withColumn("is_open", F.col("is_open").cast(T.BooleanType()))
    business = business.withColumn("categories", F.split("categories", ", ", limit=-1))
    start_columnNames = business.columns
    business = business.select(
        flattenStructSchema(business.schema, None, ["attributes"])
    )
    new_attribute_columnNames = set(business.columns) - set(start_columnNames)
    business = convertJSONColumnsOrCast(
        business,
        new_attribute_columnNames,
        json_schema=T.MapType(T.StringType(), T.BooleanType()),
        fallback_type=T.BooleanType(),
    )
    business = business.select(flattenStructSchema(business.schema, None, ["hours"]))

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
        split_hours_col = F.split(business[f"hours_{day}"], "-")
        business = business.withColumn(f"hours_{day}_start", split_hours_col.getItem(0))
        business = business.withColumn(f"hours_{day}_end", split_hours_col.getItem(1))
        business = business.drop(f"hours_{day}")

    # Create Business Categories helper table
    business_categories = business.withColumn(
        "categories", F.explode(business.categories)
    ).select(["business_id", "categories"])

    # User dimension
    user = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.user"
    )
    user = user.withColumn(
        "yelping_since_timestamp",
        F.to_timestamp(user.yelping_since, "yyyy-MM-dd HH:mm:ss"),
    )
    user = user.withColumn(
        "yelping_since", F.to_date(user.yelping_since, "yyyy-MM-dd HH:mm:ss")
    )
    user = user.withColumn(
        "yelping_since_week",
        F.to_date(F.date_trunc("week", user.yelping_since), "yyyy-MM-dd HH:mm:ss"),
    )

    dimension_table_names = [
        "business",
        "business_categories",
        "user",
    ]
    for table_name, df in list(
        zip(dimension_table_names, [business, business_categories, user])
    ):
        df.printSchema()
        df.show()
        iceberg_output_table = ".".join(
            [
                spark.conf.get("spark.sql.defaultCatalog"),
                known_args.pipeline,
                namespace,
                table_name,
            ]
        )

        _logger.info(f"Writing to Iceberg table at {output_iceberg_table}")
        df.writeTo(iceberg_output_table).partitionedBy(
            F.bucket(known_args.buckets, f"{table_name.split('_')[0]}_id")
        ).using("iceberg").option("write.object-storage.enabled", True).option(
            "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
        ).createOrReplace()

        _logger.info(f"Finished writing table {output_iceberg_table} to Iceberg format")

    business = business.select(
        [F.col(c).alias("business_" + c) for c in business.columns]
    )
    user = user.select([F.col(c).alias("user_" + c) for c in user.columns])

    # "One Big Table"-style preemptive joins
    business_checkin = checkin.join(
        business, checkin.business_id == business.business_business_id
    )
    user_business_review = review.join(
        business,
        review.business_id == business.business_business_id,
    ).join(user, review.user_id == user.user_user_id)
    user_business_tip = tip.join(
        business, tip.business_id == business.business_business_id
    ).join(user, tip.user_id == user.user_user_id)

    fact_table_names = [
        "checkin",
        "review",
        "tip",
        "user_business_review",
        "user_business_tip",
        "business_checkin",
    ]
    for table_name, df in list(
        zip(
            fact_table_names,
            [
                checkin,
                review,
                tip,
                user_business_review,
                user_business_tip,
                business_checkin,
            ],
        )
    ):
        output_iceberg_table = ".".join(
            [
                spark.conf.get("spark.sql.defaultCatalog"),
                known_args.pipeline,
                namespace,
                table_name,
            ]
        )
        df.printSchema()
        df.show()

        _logger.info(f"Writing to Iceberg table at {output_iceberg_table}")
        df.writeTo(output_iceberg_table).partitionedBy(
            F.years("date_week"),
            F.bucket(known_args.buckets, "business_id"),
        ).using("iceberg").option("write.object-storage.enabled", True).option(
            "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
        ).createOrReplace()

        _logger.info(f"Finished writing table {output_iceberg_table} to Iceberg format")

    _logger.info("Silver processing completed!")


def flattenStructSchema(
    schema: T.StructType, prefix: str = None, struct_cols: List[str] = None
) -> List[F.Column]:
    columns = []
    for f in schema.fields:
        columnName = f.name if prefix is None else f"{prefix}.{f.name}"
        if isinstance(f.dataType, T.StructType) and (
            struct_cols is None or f.name in struct_cols
        ):
            columns.extend(flattenStructSchema(f.dataType, columnName))
        else:
            columns.append(
                F.col(columnName).alias(columnName.replace(".", "_").lower())
            )
    return columns


def convertJSONColumnsOrCast(df, columnNames: List[str], json_schema, fallback_type):
    for columnName in columnNames:
        json_columnName = f"{columnName}_json"
        df = df.withColumn(json_columnName, F.from_json(columnName, json_schema))
        df = drop_fully_null_columns(df, [json_columnName])
        if json_columnName not in df.columns:
            cast_columnName = f"{columnName}_cast"
            df = df.withColumn(cast_columnName, F.col(columnName).cast(fallback_type))
            df = drop_fully_null_columns(df, [cast_columnName])
            if cast_columnName in df.columns:
                df = df.drop(columnName)
                df = df.withColumnRenamed(cast_columnName, columnName)
                _logger.info(f"Converted {columnName} to {fallback_type}")
            else:
                _logger.info(f"Converted {columnName} to StringType")
        else:
            df = df.drop(columnName)
            df = df.withColumnRenamed(json_columnName, columnName)
            _logger.info(f"Converted {columnName} to {json_schema}")
    return df


def drop_fully_null_columns(df, check_columns=[]):
    """Drops DataFrame columns that are fully null
    (i.e. the maximum value is null)
    https://stackoverflow.com/a/65361020/22623325

    Arguments:
        df {spark DataFrame} -- spark dataframe
        check_columns {list} -- list of columns to check for nulls

    Returns:
        spark DataFrame -- dataframe with fully null columns removed
    """
    if len(check_columns) > 0:
        # drop columns for which the max is None
        rows_with_data = (
            df.select(*check_columns)
            .groupby()
            .agg(
                *[F.max(F.col(c).cast(T.StringType())).alias(c) for c in check_columns]
            )
            .take(1)[0]
        )
        cols_to_drop = [
            c for c, const in rows_with_data.asDict().items() if const is None
        ]
        df = df.drop(*cols_to_drop)
    return df
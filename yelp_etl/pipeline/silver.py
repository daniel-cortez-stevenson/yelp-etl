import logging

from pyspark.sql import functions as F

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def silver(
    spark,
    known_args: dict,
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
    # TODO: Date parsing will fail without this.
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    _logger.info("Set spark.sql.legacy.timeParserPolicy to LEGACY")

    checkin = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.checkin"
    )
    checkin = checkin.withColumn("date", F.explode(F.split("date", ", ", limit=-1)))
    checkin = checkin.withColumn(
        "checkin_id", F.sha1(F.concat(checkin.business_id, checkin.date))
    )
    checkin = checkin.withColumn(
        "date_timestamp", F.to_timestamp(checkin.date, "yyyy-MM-dd HH:mm:ss")
    )
    checkin = checkin.withColumn("date", F.to_date(checkin.date, "yyyy-MM-dd HH:mm:ss"))
    checkin = checkin.withColumn(
        "date_week",
        F.to_date(F.date_trunc("week", checkin.date), "yyyy-MM-dd HH:mm:ss"),
    )

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

    tip = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.tip"
    )
    tip = tip.withColumn("date_timestamp", F.to_timestamp(tip.date, "yyyy-MM-dd"))
    tip = tip.withColumn("date", F.to_date(tip.date))
    tip = tip.withColumn(
        "date_week", F.to_date(F.date_trunc("week", tip.date), "yyyy-MM-dd HH:mm:ss")
    )

    # TODO: Unnest JSON columns for subject & object entity types so analysts like me.
    business = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.business"
    )
    business = business.select(
        [F.col(c).alias("business_" + c) for c in business.columns]
    )

    user = spark.table(
        f"{spark.conf.get('spark.sql.defaultCatalog')}.bronze.{namespace}.user"
    )
    user = user.select([F.col(c).alias("user_" + c) for c in user.columns])

    # "One Big Table"-style preemptive joins
    # TODO: Optimize this join or omit it from the OBT schema.
    # Fails with OOM on the docker-compose standalone set up.
    # business_checkin = checkin.join(
    #     business, checkin.business_id == business.business_business_id
    # )
    business_user_review = review.join(user, review.user_id == user.user_user_id).join(
        business,
        review.business_id == business.business_business_id,
    )
    business_user_tip = tip.join(user, tip.user_id == user.user_user_id).join(
        business, tip.business_id == business.business_business_id
    )

    table_names = [
        "checkin",
        "review",
        "tip",
        "business_user_review",
        "business_user_tip",
    ]  # "business_checkin",
    for table_name, df in list(
        zip(
            table_names, [checkin, review, tip, business_user_review, business_user_tip]
        )
    ):  # , business_checkin
        df.printSchema()
        df.show()
        output_table = f"{spark.conf.get('spark.sql.defaultCatalog')}.{known_args.pipeline}.{namespace}.{table_name}"
        _logger.info(f"Writing to {output_table}")
        df.writeTo(output_table).using(  # TODO: .partitionedBy("date_week")
            "iceberg"
        ).option("write.object-storage.enabled", True).option(
            "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
        ).createOrReplace()

    _logger.info("Silver processing completed!")

    # TODO: Is Activity Schema useful to analysts?
    # activities = (
    #     business_checkin.unionByName(business_user_review, allowMissingColumns=True)
    #     .unionByName(business_user_tip, allowMissingColumns=True)
    #     .sort("date_timestamp")
    # )
    # activity_output_table = f"{spark.conf.get('spark.sql.defaultCatalog')}.{known_args.pipeline}.{namespace}.activity"
    # TODO: Fix linting error
    # activities.writeTo(f"{spark.conf.get('spark.sql.defaultCatalog')}.{known_args.pipeline}.{namespace}.activities").partitionedBy("date_week").using(
    #     "iceberg"
    # ).option("write.object-storage.enabled", True).option(
    #     "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
    # ).createOrReplace()
    # activity = spark.table(activity_output_table)
    # activity.printSchema()
    # activity.show()

import logging
import os

from pyspark.sql import functions as F

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def bronze(
    spark,
    known_args,
    input_path: str,
    namespace: str,
    table_name: str,
    bucket_col: str,
):
    """
    Convert individual Yelp JSON files to Apache Iceberg format.

    :param spark: SparkSession.
    :param known_args: Spark and pipeline setup args to executable.
    :param input_path: Path prefix for all Yelp Dataset JSON files.
    :param namespace: Iceberg "database" in the cluster-configured Catalog.
    :param table_name: Name of the Iceberg table.
    :param bucket_col: Column of the Iceberg table you will be aggregating on.
    """
    spark.conf.set(
        "spark.sql.legacy.bucketedTableScan.outputOrdering", True
    )  # Useful if 1 file per bucket
    # TODO: Read --table_name='business' with a predefined schema
    input_json_file = os.path.join(
        input_path, f"yelp_academic_dataset_{table_name}.json"
    )
    output_iceberg_table = ".".join(
        [
            spark.conf.get("spark.sql.defaultCatalog"),
            known_args.pipeline,
            namespace,
            table_name,
        ]
    )
    _logger.info(f"Reading JSON data from {input_json_file}")
    df = spark.read.json(input_json_file)
    df.printSchema()
    df.show()

    _logger.info(f"Writing to Iceberg table at {output_iceberg_table}")
    # Bucketing reference: https://towardsdatascience.com/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53
    df.writeTo(output_iceberg_table).partitionedBy(
        F.bucket(known_args.buckets, bucket_col)
    ).using("iceberg").option("write.object-storage.enabled", True).option(
        "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
    ).createOrReplace()

    _logger.info(f"Finished writing table {output_iceberg_table} to Iceberg format")

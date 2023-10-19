import logging
import os

from pyspark.sql.functions import bucket

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def bronze(
    spark,
    known_args: dict,
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
    input_json_file = os.path.join(
        input_path, f"yelp_academic_dataset_{table_name}.json"
    )
    # TODO: Fix linting error
    output_iceberg_table = f"{spark.conf.get('spark.sql.defaultCatalog')}.{known_args.pipeline}.{namespace}.{table_name}"
    # Avoid too small or too large file sizes & speed up aggregations.
    num_buckets = 40

    _logger.info(f"Reading JSON data from {input_json_file}")
    df = spark.read.json(input_json_file)

    _logger.info(f"Writing to Iceberg table at {output_iceberg_table}")

    df.printSchema()
    df.show()
    df.writeTo(output_iceberg_table).partitionedBy(
        bucket(num_buckets, bucket_col)
    ).using("iceberg").option("write.object-storage.enabled", True).option(
        "write.data.path", spark.conf.get("spark.sql.catalog.lake.warehouse")
    ).createOrReplace()

    _logger.debug(f"Finished writing table {output_iceberg_table} to Iceberg format")

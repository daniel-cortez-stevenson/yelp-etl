import logging

from pyspark.sql import SparkSession

logging.basicConfig(level="INFO")
_logger = logging.getLogger(__name__)


def setup_spark_session():
    """
    Set up and return a SparkSession.

    :return: Configured SparkSession.
    """
    _logger.info("Started setting up SparkSession")
    spark = SparkSession.builder.appName("Yelp ETL in Iceberg data lake").getOrCreate()
    # TODO: Sensible runtime Spark Session configuration.
    _logger.info("Finished setting up SparkSession")
    return spark

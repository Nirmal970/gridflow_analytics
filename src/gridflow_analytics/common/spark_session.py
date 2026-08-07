from pyspark.sql import SparkSession

from gridflow_analytics.config.config import APP_NAME
from gridflow_analytics.common.logger import logger


def get_spark_session() -> SparkSession:

    try:

        logger.info("Creating Spark Session...")

        spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

        logger.info("Spark Session created successfully.")

        return spark

    except Exception:

        logger.exception("Failed to create Spark Session.")

        raise
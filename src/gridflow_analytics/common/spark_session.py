from pyspark.sql import SparkSession

from gridflow_analytics.config.config import APP_NAME


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
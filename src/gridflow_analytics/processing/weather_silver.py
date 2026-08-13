from pyspark.sql.functions import col,to_timestamp

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from pyspark.dbutils import DBUtils

from gridflow_analytics.config.config import BRONZE_CONTAINER,STORAGE_ACCOUNT,SILVER_CONTAINER

from gridflow_analytics.common.silver_utils import read_bronze_observations,add_processed_timestamp,merge_delta



def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/weather"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/weather"

        logger.info("Starting Weather Silver processing.")

        df = read_bronze_observations(spark,bronze_path)

        df = df.select(
            to_timestamp(col("timestamp")).alias("timestamp"),
            col("temperature").cast("double").alias("temperature"),
            col("apparent_temperature").cast("double").alias("apparent_temperature"),
            col("relative_humidity").cast("double").alias("relative_humidity"),
            col("dewpoint").cast("double").alias("dewpoint"),
            col("wind_speed").cast("double").alias("wind_speed"),
            col("wind_direction").cast("double").alias("wind_direction"),
            col("surface_pressure").cast("double").alias("surface_pressure"),
            col("cloud_cover").cast("double").alias("cloud_cover"),
            col("precipitation").cast("double").alias("precipitation"),
            col("ghi").cast("double").alias("ghi"),
            col("direct_radiation").cast("double").alias("direct_radiation"),
            col("dni").cast("double").alias("dni"),
            col("dhi").cast("double").alias("dhi")
        )

        df = df.filter(col("timestamp").isNotNull())

        df = df.dropDuplicates(["timestamp"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.timestamp = source.timestamp")

        logger.info("Weather Silver processing completed successfully.")

    except Exception:

        logger.exception("Weather Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
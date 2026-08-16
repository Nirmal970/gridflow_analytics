from pyspark.sql.functions import col,explode_outer,to_timestamp

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

        df = df.select(col("state").cast("string").alias("state"),explode_outer(col("points")).alias("point"))

        df = df.select(col("state"),to_timestamp(col("point.ts_utc")).alias("timestamp"),col("point.temperature_2m").cast("double").alias("temperature"),col("point.apparent_temperature").cast("double").alias("apparent_temperature"),col("point.relative_humidity_2m").cast("double").alias("relative_humidity"),col("point.dewpoint_2m").cast("double").alias("dewpoint"),col("point.wind_speed_10m").cast("double").alias("wind_speed"),col("point.wind_direction_10m").cast("double").alias("wind_direction"),col("point.surface_pressure").cast("double").alias("surface_pressure"),col("point.cloud_cover").cast("double").alias("cloud_cover"),col("point.precipitation").cast("double").alias("precipitation"),col("point.ghi_w_m2").cast("double").alias("ghi"),col("point.direct_radiation").cast("double").alias("direct_radiation"),col("point.dni_w_m2").cast("double").alias("dni"),col("point.dhi_w_m2").cast("double").alias("dhi"))

        df = df.filter(col("timestamp").isNotNull() & col("state").isNotNull())

        df = df.dropDuplicates(["state","timestamp"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.state = source.state AND target.timestamp = source.timestamp")

        logger.info("Weather Silver processing completed successfully.")

    except Exception:

        logger.exception("Weather Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
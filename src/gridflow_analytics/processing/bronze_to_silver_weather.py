from pyspark.sql.functions import col, current_timestamp

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.logger import logger
from pyspark.dbutils import DBUtils

from gridflow_analytics.config.config import (
    BRONZE_CONTAINER,
    BRONZE_WEATHER_PATH,
    SILVER_CONTAINER,
    SILVER_WEATHER_PATH,
    STORAGE_ACCOUNT,
)


def extract(spark, city: str):

    try:

        bronze_path =f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"{BRONZE_WEATHER_PATH}/{city}"

        logger.info(f"Reading Bronze data from: {bronze_path}")

        df = spark.read.json(bronze_path)

        record_count = df.count()

        logger.info(f"Bronze Record Count: {record_count}")

        if record_count == 0:
            raise ValueError("Bronze layer contains no records.")

        return df

    except Exception:

        logger.exception("Failed during Bronze extraction.")

        raise


def transform(df):

    try:

        logger.info("Transforming Bronze data into Silver format...")

        silver_df = df.select(
            col("latitude"),
            col("longitude"),
            col("elevation"),
            col("generationtime_ms"),
            col("timezone"),
            col("timezone_abbreviation"),
            col("utc_offset_seconds"),
            col("current.time").alias("weather_time"),
            col("current.interval").alias("interval_seconds"),
            col("current.temperature_2m").alias("temperature_c"),
            col("current.apparent_temperature").alias("apparent_temperature_c"),
            col("current.relative_humidity_2m").alias("relative_humidity"),
            col("current.precipitation").alias("precipitation_mm"),
            col("current.rain").alias("rain_mm"),
            col("current.wind_speed_10m").alias("wind_speed_kmh"),
            col("current.wind_direction_10m").alias("wind_direction_deg"),
            col("current.surface_pressure").alias("surface_pressure_hpa"),
            current_timestamp().alias("processed_timestamp"),
        )

        logger.info(f"Silver Record Count: {silver_df.count()}")

        return silver_df

    except Exception:

        logger.exception("Failed during transformation.")

        raise


def load(df):

    try:

        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"{SILVER_WEATHER_PATH}"

        logger.info(f"Writing Silver data to: {silver_path}")
        df.write.format("delta").mode("append").save(silver_path)

        logger.info("Silver layer written successfully.")

    except Exception:

        logger.exception("Failed while writing Silver layer.")

        raise


def main(city: str = "hyderabad"):

    spark = get_spark_session()
    dbutils = DBUtils(spark)

    try:

        logger.info("Starting Bronze -> Silver Weather ETL")

        configure_adls(spark,dbutils)

        bronze_df = extract(spark, city)

        silver_df = transform(bronze_df)

        load(silver_df)

        logger.info("Bronze -> Silver ETL completed successfully.")

    except Exception:

        logger.exception("Weather ETL failed.")

        raise

    finally:

        logger.info("Completed Successfully")


if __name__ == "__main__":
    main()
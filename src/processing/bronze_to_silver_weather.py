from pyspark.sql.functions import col, current_timestamp

from src.common.spark_session import get_spark_session
from src.common.adls_auth import configure_adls

from src.config.config import (
    BRONZE_CONTAINER,
    BRONZE_WEATHER_PATH,
    SILVER_CONTAINER,
    SILVER_WEATHER_PATH,
    STORAGE_ACCOUNT,
)


def extract(spark, city: str):

    bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"{BRONZE_WEATHER_PATH}/{city}"

    print(f"Reading Bronze Data : {bronze_path}")

    return spark.read.json(bronze_path)


def transform(df):

    return df.select(
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

        current_timestamp().alias("processed_timestamp")
    )


def load(df):

    silver_path = (
        f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"{SILVER_WEATHER_PATH}"
    )

    print(f"Writing Silver Data : {silver_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    print("Silver layer written successfully.")


def main():

    spark = get_spark_session()

    # Temporary need to make it dynamic
    city = "hyderabad"
    
    configure_adls(spark)

    bronze_df = extract(spark, city)

    print(f"Bronze Record Count : {bronze_df.count()}")

    silver_df = transform(bronze_df)

    print(f"Silver Record Count : {silver_df.count()}")

    load(silver_df)

    print("Bronze -> Silver ETL Completed Successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
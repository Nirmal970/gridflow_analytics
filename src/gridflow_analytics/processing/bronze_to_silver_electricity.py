from pyspark.sql.functions import (col,explode,current_timestamp,from_json,to_timestamp)

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.logger import logger

from pyspark.dbutils import DBUtils

from gridflow_analytics.config.config import (BRONZE_CONTAINER,SILVER_CONTAINER,STORAGE_ACCOUNT)

def extract(spark):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"raw/electricity/demand"

        logger.info(f"Reading Electricity Bronze Data: {bronze_path}")

        df = spark.read.json(bronze_path)

        return df

    except Exception:

        logger.exception("Failed during Electricity Bronze extraction.")

        raise

def transform(spark, df):

    try:

        logger.info("Transforming Electricity Bronze data into Silver format...")

        json_schema = spark.read.json(df.select("raw_response").rdd.map(lambda row: row.raw_response)).schema

        parsed_df = df.withColumn("api_response",from_json(col("raw_response"), json_schema))

        silver_df = (
            parsed_df
            .withColumn("item", explode(col("api_response.items")))
            .withColumn("point", explode(col("item.points")))
            .select(
                to_timestamp(col("point.timestamp")).alias("timestamp"),
                col("point.state").alias("state"),
                col("point.region").alias("region"),
                col("point.demand_mw").cast("double").alias("demand_mw"),
                col("point.peak_mw").cast("double").alias("peak_mw"),
                col("point.frequency_hz").cast("double").alias("frequency_hz"),
                col("point.source").alias("source"),
                col("point.source_type").alias("source_type"),
                col("point.rolling_peak_24mo_mw")
                    .cast("double")
                    .alias("rolling_peak_24mo_mw"),
                col("point.installed_in_state_mw")
                    .cast("double")
                    .alias("installed_in_state_mw"),
                col("point.allocated_share_mw")
                    .cast("double")
                    .alias("allocated_share_mw"),
                current_timestamp().alias("processed_timestamp"),
            )
        )

        logger.info(f"Silver Record Count: {silver_df.count()}")

        return silver_df

    except Exception:

        logger.exception("Failed during Electricity Silver transformation.")

        raise

def load(df):

    try:

        silver_path =f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"electricity/demand"

        logger.info(f"Writing Electricity Silver Data: {silver_path}")

        df.write.format("delta").mode("overwrite").save(silver_path)

        logger.info("Electricity Silver layer written successfully.")

    except Exception:

        logger.exception("Failed while writing Electricity Silver layer.")

        raise

def main():

    spark = get_spark_session()
    
    dbutils = DBUtils(spark)

    try:
      

        configure_adls(spark,dbutils)

        bronze_df = extract(spark)

        logger.info(f"Bronze Record Count: {bronze_df.count()}")

        silver_df = transform(spark, bronze_df)

        logger.info(f"Silver Record Count: {silver_df.count()}")

        load(silver_df)

        logger.info("Electricity Bronze -> Silver ETL Completed Successfully.")

    finally:

        logger.info("Completed Successfully")

        pass

if __name__ == "__main__":
    main()
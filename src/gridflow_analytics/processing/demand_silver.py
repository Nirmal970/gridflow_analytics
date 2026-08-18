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

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/demand"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/demand"

        logger.info("Starting State Demand Silver processing.")

        df = read_bronze_observations(spark,bronze_path,silver_path)

        df = df.select(col("state").cast("string").alias("state"),col("ingestion_timestamp"),explode_outer(col("points")).alias("point"))

        df = df.select(
            col("state"),
            to_timestamp(col("point.timestamp")).alias("timestamp"),
            col("point.region").cast("string").alias("region"),
            col("point.demand_mw").cast("double").alias("demand_mw"),
            col("point.peak_mw").cast("double").alias("peak_mw"),
            col("point.frequency_hz").cast("double").alias("frequency_hz"),
            col("point.source").cast("string").alias("source"),
            col("point.source_type").cast("string").alias("source_type"),
            col("point.rolling_peak_24mo_mw").cast("double").alias("rolling_peak_24mo_mw"),
            col("point.installed_in_state_mw").cast("double").alias("installed_in_state_mw"),
            col("point.allocated_share_mw").cast("double").alias("allocated_share_mw"),
            col("ingestion_timestamp")
        )

        df = df.filter(col("timestamp").isNotNull() &col("state").isNotNull() & col("demand_mw").isNotNull() & col("source").isNotNull())

        df = df.dropDuplicates(["state","timestamp","source"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.state = source.state AND target.timestamp = source.timestamp AND target.source = source.source")

        logger.info("State Demand Silver processing completed successfully.")

    except Exception:

        logger.exception("State Demand Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
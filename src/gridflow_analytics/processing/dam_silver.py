from pyspark.sql.functions import col,to_timestamp

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from pyspark.dbutils import DBUtils

from gridflow_analytics.config.config import BRONZE_CONTAINER,STORAGE_ACCOUNT,SILVER_CONTAINER

from gridflow_analytics.processing.silver_utils import read_bronze_observations,add_processed_timestamp,merge_delta


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/dam"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/dam"

        logger.info("Starting DAM Silver processing.")

        df = read_bronze_observations(spark,bronze_path)

        df = df.select(
            to_timestamp(col("timestamp")).alias("timestamp"),
            col("market_type").cast("string").alias("market_type"),
            col("region").cast("string").alias("region"),
            col("purchase_bid_mw").cast("double").alias("purchase_bid_mw"),
            col("sell_bid_mw").cast("double").alias("sell_bid_mw"),
            col("mcv_mw").cast("double").alias("mcv_mw"),
            col("mcp_rs_mwh").cast("double").alias("mcp_rs_mwh"),
            col("source").cast("string").alias("source"),
            to_timestamp(col("collected_at")).alias("collected_at")
        )

        df = df.filter(col("timestamp").isNotNull() & col("market_type").isNotNull())

        df = df.dropDuplicates(["timestamp","market_type","region"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.timestamp = source.timestamp AND target.market_type = source.market_type AND target.region = source.region")

        logger.info("DAM Silver processing completed successfully.")

    except Exception:

        logger.exception("DAM Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
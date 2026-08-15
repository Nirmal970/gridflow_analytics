from pyspark.sql.functions import col,to_date,avg,max,min,count
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.silver_utils import merge_delta

from gridflow_analytics.config.config import SILVER_CONTAINER,GOLD_CONTAINER,STORAGE_ACCOUNT


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/dam"
        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/market_daily"

        logger.info("Starting Market Gold processing.")

        df = spark.read.format("delta").load(silver_path)

        df = df.withColumn("date",to_date(col("timestamp")))

        gold_df = df.groupBy("date","market_type","region").agg(avg("purchase_bid_mw").alias("avg_purchase_bid_mw"),
        max("purchase_bid_mw").alias("max_purchase_bid_mw"),avg("sell_bid_mw").alias("avg_sell_bid_mw"),max("sell_bid_mw").alias("max_sell_bid_mw"),avg("mcv_mw").alias("avg_mcv_mw"),
        max("mcv_mw").alias("max_mcv_mw"),avg("mcp_rs_mwh").alias("avg_mcp_rs_mwh"),max("mcp_rs_mwh").alias("max_mcp_rs_mwh"),
        min("mcp_rs_mwh").alias("min_mcp_rs_mwh"),count("*").alias("market_observations"))

        gold_df = gold_df.withColumn("avg_bid_gap_mw",col("avg_purchase_bid_mw") - col("avg_sell_bid_mw"))

        gold_df = gold_df.withColumn("avg_executed_vs_purchase_pct",col("avg_mcv_mw") / col("avg_purchase_bid_mw") * 100)

        merge_delta(spark,gold_df,gold_path,"target.date <=> source.date AND target.market_type <=> source.market_type AND target.region <=> source.region")

        logger.info("Market Gold processing completed successfully.")

    except Exception:

        logger.exception("Market Gold processing failed.")

        raise


if __name__ == "__main__":

    main()
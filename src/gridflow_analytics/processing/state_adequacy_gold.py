from pyspark.sql.functions import col,when,max,min,avg,first
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.gold_utils import read_silver_incremental, save_gold
from gridflow_analytics.config.config import SILVER_CONTAINER,GOLD_CONTAINER,STORAGE_ACCOUNT


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/psp"
        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/state_adequacy"

        logger.info("Starting State Adequacy Gold processing.")

        df = read_silver_incremental(spark, silver_path, gold_path)

        if df.count() == 0:
            logger.info("No new data to process. Skipping.")
            return

        gold_df = df.groupBy("date","region","state").agg(max("peak_demand_mw").alias("peak_demand_mw"),max("peak_demand_met_mw").alias("peak_demand_met_mw"),
        max("peak_shortage_mw").alias("peak_shortage_mw"),max("energy_met_mu").alias("energy_met_mu"),max("energy_shortage_mu").alias("energy_shortage_mu"),
        max("frequency_min_hz").alias("frequency_min_hz"),max("frequency_max_hz").alias("frequency_max_hz"),
        max("frequency_avg_hz").alias("frequency_avg_hz"),first("source_url",True).alias("source_url"))

        gold_df = gold_df.withColumn("peak_demand_gap_mw",when(col("peak_demand_met_mw").isNotNull(),col("peak_demand_mw") - col("peak_demand_met_mw")))

        gold_df = gold_df.withColumn("demand_met_pct",when((col("peak_demand_mw") > 0) & col("peak_demand_met_mw").isNotNull(),col("peak_demand_met_mw") / col("peak_demand_mw") * 100))

        gold_df = gold_df.withColumn("energy_shortage_pct",when((col("energy_met_mu") + col("energy_shortage_mu")) > 0,
        col("energy_shortage_mu") / (col("energy_met_mu") + col("energy_shortage_mu")) * 100))

        gold_df = gold_df.withColumn("frequency_range_hz",when(col("frequency_min_hz").isNotNull() & col("frequency_max_hz").isNotNull(),col("frequency_max_hz") - col("frequency_min_hz")))

        merge_delta(gold_df, gold_path, "target.date <=> source.date AND target.region <=> source.region AND target.state <=> source.state")

        logger.info("State Adequacy Gold processing completed successfully.")

    except Exception:

        logger.exception("State Adequacy Gold processing failed.")

        raise


if __name__ == "__main__":

    main()
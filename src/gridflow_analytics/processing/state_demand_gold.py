from pyspark.sql.functions import col,to_date,avg,max,min,count,when
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.silver_utils import *
from gridflow_analytics.config.config import SILVER_CONTAINER,GOLD_CONTAINER,STORAGE_ACCOUNT
from datetime import datetime,timezone,timedelta


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/demand"
        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/state_demand"

        logger.info("Starting State Demand Gold processing.")

        df = read_silver_incremental(spark, silver_path, gold_path)

        if df.count() == 0:
            logger.info("No new data to process. Skipping.")
            return

        df = df.withColumn("date",to_date(col("timestamp")))

        gold_df = df.groupBy("state","region","date").agg(avg("demand_mw").alias("avg_demand_mw"),max("demand_mw").alias("peak_demand_mw"),
        min("demand_mw").alias("min_demand_mw"),
        (max("demand_mw") - min("demand_mw")).alias("demand_range_mw"),avg("frequency_hz").alias("avg_frequency_hz"),count("*").alias("observations"),
        count(when(col("source_type") == "official",True)).alias("official_observations"),count(when(col("source_type") == "modeled",True)).alias("modeled_observations"),
        max("installed_in_state_mw").alias("installed_in_state_mw"),max("allocated_share_mw").alias("allocated_share_mw"),max("rolling_peak_24mo_mw").alias("rolling_peak_24mo_mw"))
        
        gold_df = gold_df.withColumn("ingestion_timestamp",lit(datetime.now(timezone.utc)))

        merge_delta(gold_df, gold_path, "target.state <=> source.state AND target.region <=> source.region AND target.date <=> source.date")

        logger.info("State Demand Gold processing completed successfully.")

    except Exception:

        logger.exception("State Demand Gold processing failed.")

        raise


if __name__ == "__main__":

    main()
from pyspark.sql.functions import col,to_date,avg,max,min,count,when,coalesce,lit,abs,sum as spark_sum
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

        demand_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/national_demand"
        fuelmix_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/national_fuelmix"
        frequency_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/frequency"
        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/national_grid"

        logger.info("Starting National Grid Gold processing.")

        demand_df = read_silver_incremental(spark, demand_path, gold_path)

        fuelmix_df = read_silver_incremental(spark, fuelmix_path, gold_path)

        frequency_df = read_silver_incremental(spark, frequency_path, gold_path)

        if demand_df.count() == 0 or fuelmix_df.count() == 0 or frequency_df.count() == 0:
            logger.info("No new data to process. Skipping.")
            return
        
        demand_max = demand_df.select(max("ingestion_timestamp").cast("timestamp")).first()[0]
        fuelmix_max = fuelmix_df.select(max("ingestion_timestamp").cast("timestamp")).first()[0]
        frequency_max = frequency_df.select(max("ingestion_timestamp").cast("timestamp")).first()[0]

        max_silver_ingestion = sorted([demand_max, fuelmix_max, frequency_max])[-1]
        

        demand_daily = demand_df.groupBy(to_date(col("timestamp")).alias("date")).agg(avg("demand_mw").alias("avg_national_demand_mw"),\
        max("demand_mw").alias("peak_national_demand_mw"),min("demand_mw").alias("min_national_demand_mw"),count("*").alias("demand_observations"))

        frequency_daily = frequency_df.groupBy(to_date(col("timestamp")).alias("date")).agg(avg("frequency_hz").alias("avg_frequency_hz"),min("frequency_hz").alias("min_frequency_hz"),
        max("frequency_hz").alias("max_frequency_hz"),count("*").alias("frequency_observations"))

        fuel_daily = fuelmix_df.groupBy(to_date(col("timestamp")).alias("date"),"fuel").agg(avg("mw").alias("avg_mw"))

        fuel_total = fuel_daily.groupBy("date").agg(spark_sum("avg_mw").alias("total_generation_mw"))

        fuel_pivot = fuel_daily.groupBy("date").pivot("fuel",["gas","hydro","nuclear","solar","thermal","wind"]).agg(avg("avg_mw"))

        fuel_pivot = fuel_pivot.withColumn("renewable_generation_mw",coalesce(col("hydro"),lit(0.0)) + coalesce(col("solar"),lit(0.0)) + coalesce(col("wind"),lit(0.0)))

        fuel_pivot = fuel_pivot.withColumn("thermal_generation_mw",coalesce(col("thermal"),lit(0.0)))

        fuel_pivot = fuel_pivot.join(fuel_total,["date"],"left")

        fuel_pivot = fuel_pivot.withColumn("renewable_share_pct",when(col("total_generation_mw") > 0,col("renewable_generation_mw") / col("total_generation_mw") * 100))

        fuel_pivot = fuel_pivot.withColumn("thermal_share_pct",when(col("total_generation_mw") > 0,col("thermal_generation_mw") / col("total_generation_mw") * 100))

        gold_df = demand_daily.join(frequency_daily,["date"],"left").join(fuel_pivot,["date"],"left")

        gold_df = gold_df.withColumn("frequency_deviation_hz",when(col("avg_frequency_hz").isNotNull(),abs(col("avg_frequency_hz") - 50.0)))
        
        gold_df = gold_df.withColumn("ingestion_timestamp",lit(max_silver_ingestion))

        merge_delta(spark,gold_df, gold_path, "target.date <=> source.date")

        logger.info("National Grid Gold processing completed successfully.")

    except Exception:

        logger.exception("National Grid Gold processing failed.")

        raise


if __name__ == "__main__":

    main()
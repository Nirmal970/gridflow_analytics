from pyspark.sql.functions import col,date_trunc,avg,max,min,count
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

        demand_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/demand"
        weather_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/weather"
        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/state_weather_demand"

        logger.info("Starting State Weather Demand Gold processing.")

        demand_df = spark.read.format("delta").load(demand_path).filter(col("source_type") == "official")

        weather_df = spark.read.format("delta").load(weather_path)

        demand_hourly = demand_df.groupBy("state",date_trunc("hour",col("timestamp")).alias("hour")).agg(avg("demand_mw").alias("avg_demand_mw"),max("demand_mw").alias("peak_demand_mw"),min("demand_mw").alias("min_demand_mw"),count("*").alias("demand_observations"))

        weather_hourly = weather_df.groupBy("state",date_trunc("hour",col("timestamp")).alias("hour")).agg(avg("temperature").alias("temperature"),avg("apparent_temperature").alias("apparent_temperature"),avg("relative_humidity").alias("relative_humidity"),avg("dewpoint").alias("dewpoint"),avg("wind_speed").alias("wind_speed"),avg("surface_pressure").alias("surface_pressure"),avg("cloud_cover").alias("cloud_cover"),avg("precipitation").alias("precipitation"),avg("ghi").alias("ghi"),avg("direct_radiation").alias("direct_radiation"),avg("dni").alias("dni"),avg("dhi").alias("dhi"))

        gold_df = demand_hourly.join(weather_hourly,["state","hour"],"inner")

        gold_df = gold_df.withColumn("apparent_temperature_delta_c",col("apparent_temperature") - col("temperature"))

        merge_delta(spark,gold_df,gold_path,"target.state <=> source.state AND target.hour <=> source.hour")

        logger.info("State Weather Demand Gold processing completed successfully.")

    except Exception:

        logger.exception("State Weather Demand Gold processing failed.")

        raise


if __name__ == "__main__":

    main()
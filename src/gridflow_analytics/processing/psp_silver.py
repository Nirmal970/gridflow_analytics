from pyspark.sql.types import StructType,StructField,StringType,DoubleType,ArrayType
from pyspark.sql.functions import col,from_json,explode_outer,current_timestamp,to_date
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.silver_utils import merge_delta
from delta.tables import DeltaTable

from gridflow_analytics.config.config import BRONZE_CONTAINER,STORAGE_ACCOUNT,SILVER_CONTAINER


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/psp"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/psp"

        logger.info("Starting PSP Silver processing.")

        df = spark.read.json(bronze_path)

        if DeltaTable.isDeltaTable(spark,silver_path):
            max_ingestion = spark.read.format("delta").load(silver_path).selectExpr("max(ingestion_timestamp) as max_ingestion").first()["max_ingestion"]
            if max_ingestion is not None:
                df = df.filter(col("ingestion_timestamp") > max_ingestion)

        item_schema = StructType([StructField("date",StringType(),True),StructField("region",StringType(),True),StructField("state",StringType(),True),StructField("peak_demand_mw",DoubleType(),True),StructField("peak_demand_met_mw",DoubleType(),True),StructField("peak_shortage_mw",DoubleType(),True),StructField("energy_met_mu",DoubleType(),True),StructField("energy_shortage_mu",DoubleType(),True),StructField("frequency_min_hz",DoubleType(),True),StructField("frequency_max_hz",DoubleType(),True),StructField("frequency_avg_hz",DoubleType(),True),StructField("source_url",StringType(),True)])

        response_schema = StructType([StructField("items",ArrayType(item_schema),True)])

        df = df.withColumn("parsed_response",from_json(col("raw_response"),response_schema))

        df = df.select(col("ingestion_timestamp"),explode_outer(col("parsed_response.items")).alias("item"))

        df = df.select(to_date(col("item.date")).alias("date"),col("item.region").cast("string").alias("region"),col("item.state").cast("string").alias("state"),
        col("item.peak_demand_mw").cast("double").alias("peak_demand_mw"),col("item.peak_demand_met_mw").cast("double").alias("peak_demand_met_mw"),
        col("item.peak_shortage_mw").cast("double").alias("peak_shortage_mw"),col("item.energy_met_mu").cast("double").alias("energy_met_mu"),
        col("item.energy_shortage_mu").cast("double").alias("energy_shortage_mu"),col("item.frequency_min_hz").cast("double").alias("frequency_min_hz"),
        col("item.frequency_max_hz").cast("double").alias("frequency_max_hz"),col("item.frequency_avg_hz").cast("double").alias("frequency_avg_hz"),
        col("item.source_url").cast("string").alias("source_url"),col("ingestion_timestamp"))

        df = df.filter(col("date").isNotNull())

        df = df.dropDuplicates(["date","region","state","source_url"])

        df = df.withColumn("processed_timestamp",current_timestamp())

        merge_delta(spark,df,silver_path,"target.date <=> source.date AND target.region <=> source.region AND target.state <=> source.state AND target.source_url <=> source.source_url")

        logger.info("PSP Silver processing completed successfully.")

    except Exception:

        logger.exception("PSP Silver processing failed.")

        raise


if __name__ == "__main__":

    main()
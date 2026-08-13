from pyspark.sql.functions import col,to_timestamp

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

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/national_demand"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/national_demand"

        logger.info("Starting National Demand Silver processing.")

        df = read_bronze_observations(spark,bronze_path)
        
        df = df.select(explode_outer(col("points")).alias("point"))

        df = df.select(
            to_timestamp(col("ts_utc")).alias("timestamp"),
            col("demand_mw").cast("double").alias("demand_mw"),
            col("source").cast("string").alias("source"),
            col("source_url").cast("string").alias("source_url"),
            col("is_provisional").cast("boolean").alias("is_provisional"),
            to_timestamp(col("collected_at")).alias("collected_at")
        )

        df = df.filter(col("timestamp").isNotNull() & col("demand_mw").isNotNull())

        df = df.dropDuplicates(["timestamp","source"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.timestamp = source.timestamp AND target.source = source.source")

        logger.info("National Demand Silver processing completed successfully.")

    except Exception:

        logger.exception("National Demand Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
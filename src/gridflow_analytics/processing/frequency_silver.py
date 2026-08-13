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

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/frequency"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/frequency"

        logger.info("Starting Frequency Silver processing.")

        df = read_bronze_observations(spark,bronze_path)

        df = df.select(
            to_timestamp(col("timestamp")).alias("timestamp"),
            col("frequency_hz").cast("double").alias("frequency_hz"),
            col("region").cast("string").alias("region")
        )

        df = df.filter(col("timestamp").isNotNull() & col("frequency_hz").isNotNull())

        df = df.dropDuplicates(["timestamp","region"])

        df = add_processed_timestamp(df)

        merge_delta(spark,df,silver_path,"target.timestamp = source.timestamp AND target.region = source.region")

        logger.info("Frequency Silver processing completed successfully.")

    except Exception:

        logger.exception("Frequency Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
from pyspark.sql.functions import col,current_timestamp,sha2

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

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/psp"
        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/psp"

        logger.info("Starting PSP Silver processing.")

        df = spark.read.json(bronze_path)

        df = df.select(
            col("source").cast("string").alias("source"),
            col("dataset").cast("string").alias("dataset"),
            col("ingestion_timestamp").alias("ingestion_timestamp"),
            col("raw_response").cast("string").alias("raw_response")
        )

        df = df.withColumn("snapshot_hash",sha2(col("raw_response"),256))

        df = df.withColumn("processed_timestamp",current_timestamp())

        df = df.dropDuplicates(["snapshot_hash"])

        merge_delta(spark,df,silver_path,"target.snapshot_hash = source.snapshot_hash")

        logger.info("PSP Silver processing completed successfully.")

    except Exception:

        logger.exception("PSP Silver processing failed.")

        raise


if __name__ == "__main__":
    main()
import json

from datetime import datetime,timezone,timedelta

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_FREQUENCY_URL)


def fetch_frequency_data(dbutils,hours: int) -> dict:

    try:

        logger.info("Fetching grid frequency data from EnergyMap.")

        params = {"hours": hours}

        data = get(dbutils,ENERGYMAP_FREQUENCY_URL,params)

        logger.info("Grid frequency data fetched successfully from EnergyMap.")

        return data

    except Exception:

        logger.exception("Failed while fetching grid frequency data from EnergyMap.")

        raise


def write_bronze(spark,dbutils,data: dict,from_timestamp: str,to_timestamp: str,hours: int):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/frequency"

        logger.info(f"Checking existing Frequency Bronze data: {bronze_path}")

        try:

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter((col("source") == "energymap") & (col("dataset") == "grid_frequency") & (col("from_timestamp") == from_timestamp) & (col("to_timestamp") == to_timestamp)).count()

            if existing_count > 0:

                logger.info(f"Frequency Bronze data already exists for {from_timestamp} to {to_timestamp}. Skipping ingestion.")

                return

        except Exception:

            logger.info("Bronze path does not exist. Initializing new Bronze dataset.")

        raw_json = json.dumps(data)

        bronze_df = spark.createDataFrame([
            Row(
                source="energymap",
                dataset="grid_frequency",
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                hours=hours,
                ingestion_timestamp=datetime.now(timezone.utc),
                raw_response=raw_json
            )
        ])

        logger.info(f"Writing Frequency Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("Frequency Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing Frequency Bronze layer.")

        raise


def main():

    hours = 48

    to_time = datetime.now(timezone.utc)
    from_time = to_time - timedelta(hours=hours)

    from_timestamp = from_time.isoformat().replace("+00:00","Z")
    to_timestamp = to_time.isoformat().replace("+00:00","Z")

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        logger.info(f"Processing frequency data from {from_timestamp} to {to_timestamp}")

        logger.info("Starting Frequency Bronze ingestion.")

        data = fetch_frequency_data(dbutils=dbutils,hours=hours)

        write_bronze(spark=spark,dbutils=dbutils,data=data,from_timestamp=from_timestamp,to_timestamp=to_timestamp,hours=hours)

        logger.info("Frequency Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"Frequency Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
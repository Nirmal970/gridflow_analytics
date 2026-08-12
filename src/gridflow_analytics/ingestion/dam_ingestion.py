import json

from datetime import datetime,timezone,timedelta

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_DAM_URL)


def fetch_dam_data(dbutils,from_timestamp: str,to_timestamp: str) -> dict:

    try:

        logger.info("Fetching DAM data from EnergyMap.")

        params = {"market_type": "DAM","from": from_timestamp,"to": to_timestamp}

        data = get(dbutils,ENERGYMAP_DAM_URL,params)

        logger.info("DAM data fetched successfully from EnergyMap.")

        return data

    except Exception:

        logger.exception("Failed while fetching DAM data from EnergyMap.")

        raise


def write_bronze(spark,dbutils,data: dict,from_timestamp: str,to_timestamp: str):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/dam"

        logger.info(f"Checking existing DAM Bronze data: {bronze_path}")

        try:

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter((col("source") == "energymap") & (col("dataset") == "iex_dam") & (col("from_timestamp") == from_timestamp) & (col("to_timestamp") == to_timestamp)
            ).count()

            if existing_count > 0:

                logger.info(f"DAM Bronze data already exists for {from_timestamp} to {to_timestamp}. Skipping ingestion.")

                return

        except Exception:

            logger.info("Bronze path does not exist. Initializing new Bronze dataset.")

        raw_json = json.dumps(data)

        bronze_df = spark.createDataFrame([
            Row(
                source="energymap",
                dataset="iex_dam",
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                ingestion_timestamp=datetime.now(timezone.utc),
                raw_response=raw_json
            )
        ])

        logger.info(f"Writing DAM Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("DAM Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing DAM Bronze layer.")

        raise


def main():

    to_time = datetime.now(timezone.utc)
    from_time = to_time - timedelta(hours=48)

    from_timestamp = from_time.isoformat().replace("+00:00","Z")
    to_timestamp = to_time.isoformat().replace("+00:00","Z")

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        logger.info(f"Processing DAM data from {from_timestamp} to {to_timestamp}")

        logger.info("Starting DAM Bronze ingestion.")

        data = fetch_dam_data(dbutils=dbutils,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        write_bronze(spark=spark,dbutils=dbutils,data=data,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        logger.info("DAM Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"DAM Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
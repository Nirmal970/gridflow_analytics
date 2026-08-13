import json

from datetime import datetime,timezone,timedelta

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_DEMAND_TIMESERIES_URL)


def fetch_demand_data(dbutils,from_timestamp: str,to_timestamp: str) -> dict:

    try:

        logger.info("Fetching demand timeseries data from EnergyMap. CI\CD Test")

        params = {"from": from_timestamp,"to": to_timestamp}

        data = get(dbutils,ENERGYMAP_DEMAND_TIMESERIES_URL,params)

        logger.info("Demand timeseries data fetched successfully from EnergyMap.")

        return data

    except Exception:

        logger.exception("Failed while fetching demand timeseries data from EnergyMap.")

        raise


def write_bronze(spark,dbutils,data: dict,from_timestamp: str,to_timestamp: str):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/demand"

        logger.info(f"Checking existing Demand Bronze data: {bronze_path}")

        try:

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter((col("source") == "energymap") & (col("dataset") == "state_demand_timeseries") & (col("from_timestamp") == from_timestamp) &
                (col("to_timestamp") == to_timestamp)).count()

            if existing_count > 0:

                logger.info(f"Demand Bronze data already exists for {from_timestamp} to {to_timestamp}. Skipping ingestion.")

                return

        except Exception:

            logger.info("Bronze path does not exist. Initializing new Bronze dataset.")

        raw_json = json.dumps(data)

        bronze_df = spark.createDataFrame([
            Row(
                source="energymap",
                dataset="state_demand_timeseries",
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                ingestion_timestamp=datetime.now(timezone.utc),
                raw_response=raw_json
            )
        ])

        logger.info(f"Writing Demand Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("Demand Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing Demand Bronze layer.")

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

        logger.info(f"Processing demand data from {from_timestamp} to {to_timestamp}")

        logger.info("Starting Demand Bronze ingestion.")

        data = fetch_demand_data(dbutils=dbutils,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        write_bronze(spark=spark,dbutils=dbutils,data=data,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        logger.info("Demand Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"Demand Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
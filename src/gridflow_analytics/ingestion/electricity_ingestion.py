import json
import requests

from datetime import datetime, timezone

from pyspark.sql import Row

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.logger import logger
from pyspark.dbutils import DBUtils

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT)

ENERGYMAP_URL = "https://api.energymap.in/api/intelligence/demand-timeseries"

def fetch_electricity_data(api_key: str,from_timestamp: str,to_timestamp: str) -> dict:

    try:
        logger.info("Fetching electricity data from EnergyMap.")

        params = {"from": from_timestamp,"to": to_timestamp}

        headers = {"X-API-Key": api_key,"Accept": "application/json"}

        response = requests.get(ENERGYMAP_URL,params=params,headers=headers,timeout=30)

        response.raise_for_status()

        logger.info("Electricity data fetched successfully from EnergyMap.")

        return response.json()

    except Exception:

        logger.exception("Failed while fetching electricity data from EnergyMap.")

        raise

def write_bronze(spark,data: dict,from_timestamp: str,to_timestamp: str):

    try:

        raw_json = json.dumps(data)

        bronze_df = spark.createDataFrame(
            [
                Row(
                    source="energymap",
                    dataset="state_demand_timeseries",
                    from_timestamp=from_timestamp,
                    to_timestamp=to_timestamp,
                    ingestion_timestamp=datetime.now(timezone.utc),
                    raw_response=raw_json
                )
            ]
        )

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"raw/electricity/demand"

        logger.info(f"Writing Electricity Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("Electricity Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing Electricity Bronze layer.")

        raise

def main():

    spark = get_spark_session()

    try:
        dbutils = DBUtils(spark)
        configure_adls(spark,dbutils)
        api_key = dbutils.secrets.get(scope="gridflow-dev-adls",key="energymap-api-key")

        from_timestamp = "2026-08-01T00:00:00Z"
        to_timestamp = "2026-08-02T00:00:00Z"

        logger.info("Starting Electricity Bronze ingestion.")

        data = fetch_electricity_data(api_key=api_key,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        write_bronze(spark=spark,data=data,from_timestamp=from_timestamp,to_timestamp=to_timestamp)

        logger.info("Electricity Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"Electricity Bronze ingestion failed: {exc}")

        raise

if __name__ == "__main__":
    main()
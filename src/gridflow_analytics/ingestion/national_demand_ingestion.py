import json

from datetime import datetime,timezone,timedelta

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_NATIONAL_DEMAND_URL)


def fetch_national_demand_data(dbutils,hours: int) -> dict:

    try:

        logger.info("Fetching national demand data from EnergyMap.")

        params = {"hours": hours}

        data = get(dbutils,ENERGYMAP_NATIONAL_DEMAND_URL,params)

        logger.info("National demand data fetched successfully from EnergyMap.")

        return data

    except Exception:

        logger.exception("Failed while fetching national demand data from EnergyMap.")

        raise


def write_bronze(spark,dbutils,data: dict,from_timestamp: str,to_timestamp: str,hours: int):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/national_demand"

        logger.info(f"Checking existing National Demand Bronze data: {bronze_path}")

        try:

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter((col("source") == "energymap") & (col("dataset") == "national_demand_4min") & (col("from_timestamp") == from_timestamp) & (col("to_timestamp") == to_timestamp)).count()

            if existing_count > 0:

                logger.info(f"National Demand Bronze data already exists for {from_timestamp} to {to_timestamp}. Skipping ingestion.")

                return

        except Exception:

            logger.info("Bronze path does not exist. Initializing new Bronze dataset.")

        raw_json = json.dumps(data)

        bronze_df = spark.createDataFrame([
            Row(
                source="energymap",
                dataset="national_demand_4min",
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                hours=hours,
                ingestion_timestamp=datetime.now(timezone.utc),
                raw_response=raw_json
            )
        ])

        logger.info(f"Writing National Demand Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("National Demand Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing National Demand Bronze layer.")

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

        logger.info(f"Processing national demand data from {from_timestamp} to {to_timestamp}")

        logger.info("Starting National Demand Bronze ingestion.")

        data = fetch_national_demand_data(dbutils=dbutils,hours=hours)

        write_bronze(spark=spark,dbutils=dbutils,data=data,from_timestamp=from_timestamp,to_timestamp=to_timestamp,hours=hours)

        logger.info("National Demand Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"National Demand Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
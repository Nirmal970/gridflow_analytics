import json
from datetime import datetime,timezone

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from pyspark.dbutils import DBUtils
from pyspark.sql import Row
from pyspark.sql.functions import col

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_NATIONAL_DEMAND_URL,ENERGYMAP_NATIONAL_HOURS)


def fetch_national_demand(dbutils):

    try:

        logger.info("Fetching EnergyMap National Demand data.")

        data = get(dbutils,ENERGYMAP_NATIONAL_DEMAND_URL,{"hours": ENERGYMAP_NATIONAL_HOURS})

        logger.info("EnergyMap National Demand data fetched successfully.")

        return data

    except Exception:

        logger.exception("Failed to fetch EnergyMap National Demand data.")

        raise


def write_bronze(spark,dbutils,data):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/national_demand"

        logger.info(f"Checking existing National Demand Bronze data: {bronze_path}")

        if dbutils.fs.ls(bronze_path):

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter(col("dataset") == "national_demand_4min").count()

            if existing_count > 0:

                logger.info("National Demand Bronze data already exists. Skipping ingestion.")

                return

    except Exception:

        logger.info("National Demand Bronze path does not exist. Initializing new Bronze dataset.")

    raw_json = json.dumps(data)

    bronze_df = spark.createDataFrame([
        Row(
            source="energymap",
            dataset="national_demand_4min",
            ingestion_timestamp=datetime.now(timezone.utc),
            raw_response=raw_json
        )
    ])

    logger.info(f"Writing National Demand Bronze Data: {bronze_path}")

    bronze_df.write.mode("append").json(bronze_path)

    logger.info("National Demand Bronze layer written successfully.")


def main():

    spark = get_spark_session()

    dbutils = DBUtils(spark)

    try:

        logger.info("Starting EnergyMap National Demand ingestion.")

        configure_adls(spark,dbutils)

        data = fetch_national_demand(dbutils)

        write_bronze(spark,dbutils,data)

        logger.info("EnergyMap National Demand ingestion completed successfully.")

    except Exception:

        logger.exception("EnergyMap National Demand ingestion failed.")

        raise

    finally:

        logger.info("EnergyMap National Demand ingestion completed.")


if __name__ == "__main__":
    main()
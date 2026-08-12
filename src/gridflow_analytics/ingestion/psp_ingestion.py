import json
import hashlib

from datetime import datetime,timezone

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import (BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_PSP_URL)


def fetch_psp_data(dbutils) -> dict:

    try:

        logger.info("Fetching PSP data from EnergyMap.")

        data = get(dbutils,ENERGYMAP_PSP_URL)

        logger.info("PSP data fetched successfully from EnergyMap.")

        return data

    except Exception:

        logger.exception("Failed while fetching PSP data from EnergyMap.")

        raise


def write_bronze(spark,dbutils,data: dict):

    try:

        bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/psp"

        logger.info(f"Checking existing PSP Bronze data: {bronze_path}")

        raw_json = json.dumps(data,sort_keys=True)

        response_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        try:

            existing_df = spark.read.json(bronze_path)

            existing_count = existing_df.filter((col("source") == "energymap") & (col("dataset") == "posoco_psp") & (col("response_hash") == response_hash)
            ).count()

            if existing_count > 0:

                logger.info("PSP Bronze data already exists for this API response. Skipping ingestion.")

                return

        except Exception:

            logger.info("Bronze path does not exist. Initializing new Bronze dataset.")

        bronze_df = spark.createDataFrame([
            Row(
                source="energymap",
                dataset="posoco_psp",
                response_hash=response_hash,
                ingestion_timestamp=datetime.now(timezone.utc),
                raw_response=raw_json
            )
        ])

        logger.info(f"Writing PSP Bronze Data: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info("PSP Bronze layer written successfully.")

    except Exception:

        logger.exception("Failed while writing PSP Bronze layer.")

        raise


def main():

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        logger.info("Starting PSP Bronze ingestion.")

        data = fetch_psp_data(dbutils=dbutils)

        write_bronze(spark=spark,dbutils=dbutils,data=data)

        logger.info("PSP Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"PSP Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
import json

from datetime import datetime,timezone,timedelta

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.energymap_api import get
from gridflow_analytics.common.logger import logger

from gridflow_analytics.config.config import BRONZE_CONTAINER,STORAGE_ACCOUNT,ENERGYMAP_WEATHER_URL,ENERGYMAP_FETCH_HOURS,FETCH_STATES


def fetch_weather_data(dbutils,state: str,hours: int) -> dict:

    try:

        logger.info(f"Fetching weather data from EnergyMap for state={state}.")

        params = {"state":state,"hours":hours}

        data = get(dbutils,ENERGYMAP_WEATHER_URL,params)

        logger.info(f"Weather data fetched successfully from EnergyMap for state={state}.")

        return data

    except Exception:

        logger.exception(f"Failed while fetching weather data from EnergyMap for state={state}.")

        raise


def get_existing_states(spark,bronze_path: str,from_timestamp: str,to_timestamp: str) -> set:

    try:

        existing_df = spark.read.json(bronze_path)

        existing_states = existing_df.filter((col("source") == "energymap") & (col("dataset") == "weather") & (col("from_timestamp") == from_timestamp) & (col("to_timestamp") == to_timestamp)).select("state").distinct().collect()

        return {row["state"] for row in existing_states}

    except Exception:

        logger.info("Bronze path does not exist. Initializing new Weather Bronze dataset.")

        return set()


def write_bronze(spark,data_rows: list,bronze_path: str):

    if not data_rows:

        logger.info("No new Weather Bronze data to write.")

        return

    try:

        bronze_df = spark.createDataFrame(data_rows)

        logger.info(f"Writing {len(data_rows)} Weather Bronze records: {bronze_path}")

        bronze_df.write.mode("append").json(bronze_path)

        logger.info(f"Weather Bronze layer written successfully with {len(data_rows)} records.")

    except Exception:

        logger.exception("Failed while writing Weather Bronze layer.")

        raise


def main():

    hours = ENERGYMAP_FETCH_HOURS

    to_time = datetime.now(timezone.utc)
    from_time = to_time - timedelta(hours=hours)

    from_timestamp = from_time.isoformat().replace("+00:00","Z")
    to_timestamp = to_time.isoformat().replace("+00:00","Z")

    bronze_path = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/electricity/weather"

    spark = get_spark_session()

    try:

        dbutils = DBUtils(spark)

        configure_adls(spark,dbutils)

        logger.info(f"Processing weather data from {from_timestamp} to {to_timestamp} with hours={hours}")

        logger.info(f"Starting Weather Bronze ingestion for {len(FETCH_STATES)} states.")

        existing_states = get_existing_states(spark,bronze_path,from_timestamp,to_timestamp)

        if existing_states:

            logger.info(f"Found {len(existing_states)} existing Weather states for the requested window.")

        states_to_fetch = [state for state in FETCH_STATES if state not in existing_states]

        logger.info(f"Weather states requiring ingestion: {len(states_to_fetch)}")

        data_rows = []

        for state in states_to_fetch:

            logger.info(f"Starting Weather ingestion for state={state}.")

            data = fetch_weather_data(dbutils=dbutils,state=state,hours=hours)

            raw_json = json.dumps(data)

            data_rows.append(
                Row(
                    source="energymap",
                    dataset="weather",
                    state=state,
                    from_timestamp=from_timestamp,
                    to_timestamp=to_timestamp,
                    hours=hours,
                    ingestion_timestamp=datetime.now(timezone.utc),
                    raw_response=raw_json
                )
            )

        write_bronze(spark,data_rows,bronze_path)

        logger.info("Weather Bronze ingestion completed successfully.")

    except Exception as exc:

        logger.exception(f"Weather Bronze ingestion failed: {exc}")

        raise


if __name__ == "__main__":
    main()
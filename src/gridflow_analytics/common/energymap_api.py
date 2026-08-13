import requests

from gridflow_analytics.common.logger import logger
from gridflow_analytics.config.config import ENERGYMAP_REQUEST_TIMEOUT_SECONDS

from gridflow_analytics.config.config import (ENERGYMAP_SECRET_SCOPE,ENERGYMAP_API_KEY_SECRET)


def get_api_key(dbutils):

    try:

        logger.info("Reading EnergyMap API key from Databricks Secret Scope.")

        api_key = dbutils.secrets.get(scope=ENERGYMAP_SECRET_SCOPE,key=ENERGYMAP_API_KEY_SECRET)

        if not api_key:
            raise ValueError("EnergyMap API key is empty.")

        logger.info("EnergyMap API key retrieved successfully.")

        return api_key

    except Exception:

        logger.exception("Failed to retrieve EnergyMap API key.")

        raise


def get(dbutils,url,params=None):

    try:

        api_key = get_api_key(dbutils)

        headers = {"X-API-Key": api_key,"Accept": "application/json"}

        logger.info(f"Calling EnergyMap API: {url}")

        response = requests.get(url,headers=headers,params=params,timeout=ENERGYMAP_REQUEST_TIMEOUT_SECONDS)

        logger.info(f"EnergyMap API response status: {response.status_code}")

        response.raise_for_status()

        return response.json()

    except Exception:

        logger.exception(f"EnergyMap API request failed: {url}")

        raise
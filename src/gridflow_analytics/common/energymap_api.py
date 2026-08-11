import requests

from gridflow_analytics.common.logger import logger
from gridflow_analytics.config.config import ENERGYMAP_REQUEST_TIMEOUT_SECONDS


def get_api_key(dbutils):

    try:

        logger.info("Reading EnergyMap API key from Databricks Secret Scope.")

        api_key = dbutils.secrets.get(scope="gridflow-dev-adls",key="energymap-api-key")

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

        data = response.json()

        logger.info("EnergyMap API response received successfully.")

        return data

    except requests.exceptions.Timeout:

        logger.exception(f"EnergyMap API request timed out: {url}")

        raise

    except requests.exceptions.HTTPError:

        logger.exception(f"EnergyMap API HTTP error: {url}")

        raise

    except requests.exceptions.RequestException:

        logger.exception(f"EnergyMap API request failed: {url}")

        raise

    except ValueError:

        logger.exception(f"EnergyMap API returned invalid JSON: {url}")

        raise

    except Exception:

        logger.exception(f"Unexpected EnergyMap API error: {url}")

        raise
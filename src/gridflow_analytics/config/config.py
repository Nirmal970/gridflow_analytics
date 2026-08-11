APP_NAME = "GridFlow Analytics"

STORAGE_ACCOUNT = "gridflowstoragedev"

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"

BRONZE_WEATHER_PATH = "raw/weather"
SILVER_WEATHER_PATH = "weather/current"
GOLD_WEATHER_PATH = "weather"
GOLD_CONTAINER = "gold"
MODELED_SOURCE = "modeled_ml_forecast_v1"

ENERGYMAP_NATIONAL_DEMAND_URL = "https://api.energymap.in/api/intelligence/national-demand-4min"
ENERGYMAP_NATIONAL_DEMAND_HOURS = 24
ENERGYMAP_REQUEST_TIMEOUT_SECONDS = 60
ENERGYMAP_SECRET_SCOPE = "gridflow-dev-adls"
ENERGYMAP_API_KEY_SECRET = "energymap-api-key"
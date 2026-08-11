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

ENERGYMAP_BASE_URL = "https://api.energymap.in"

ENERGYMAP_SECRET_SCOPE = "gridflow-dev-adls"
ENERGYMAP_API_KEY_SECRET = "energymap-api-key"

ENERGYMAP_STATE = "gujarat"

ENERGYMAP_LOOKBACK_DAYS = 1
ENERGYMAP_WEATHER_HOURS = 24
ENERGYMAP_NATIONAL_HOURS = 24
ENERGYMAP_FREQUENCY_HOURS = 24
ENERGYMAP_PSP_LOOKBACK_DAYS = 7
ENERGYMAP_REQUEST_TIMEOUT_SECONDS = 60

ENERGYMAP_STATE_DEMAND_URL = "https://api.energymap.in/api/intelligence/demand-timeseries"

ENERGYMAP_NATIONAL_DEMAND_URL = "https://api.energymap.in/api/intelligence/national-demand-4min"

ENERGYMAP_NATIONAL_FUEL_MIX_URL = "https://api.energymap.in/api/intelligence/national-fuelmix-4min"

ENERGYMAP_FREQUENCY_URL = "https://api.energymap.in/api/intelligence/grid-frequency"

ENERGYMAP_WEATHER_URL = "https://api.energymap.in/api/intelligence/weather"

ENERGYMAP_PSP_URL = "https://api.energymap.in/api/intelligence/posoco-psp"

ENERGYMAP_DAM_URL = "https://api.energymap.in/developer/v1/market/iex/latest"
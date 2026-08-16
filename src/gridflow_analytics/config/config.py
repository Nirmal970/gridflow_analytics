APP_NAME = "GridFlow Analytics"

STORAGE_ACCOUNT = "gridflowstoragedev"

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"

BRONZE_WEATHER_PATH = "raw/weather"
SILVER_WEATHER_PATH = "weather/current"
GOLD_WEATHER_PATH = "weather"
MODELED_SOURCE = "modeled_ml_forecast_v1"


ENERGYMAP_DEMAND_TIMESERIES_URL = "https://api.energymap.in/api/intelligence/demand-timeseries"
ENERGYMAP_NATIONAL_DEMAND_URL = "https://api.energymap.in/api/intelligence/national-demand-4min"
ENERGYMAP_NATIONAL_FUELMIX_URL = "https://api.energymap.in/api/intelligence/national-fuelmix-4min"
ENERGYMAP_FREQUENCY_URL = "https://api.energymap.in/api/intelligence/grid-frequency"
ENERGYMAP_WEATHER_URL = "https://api.energymap.in/api/intelligence/weather"
ENERGYMAP_PSP_URL = "https://api.energymap.in/api/intelligence/posoco-psp"
ENERGYMAP_NATIONAL_DEMAND_HOURS = 24
ENERGYMAP_REQUEST_TIMEOUT_SECONDS = 60
ENERGYMAP_SECRET_SCOPE = "gridflow-dev-adls"
ENERGYMAP_API_KEY_SECRET = "energymap-api-key" 



ENERGYMAP_FETCH_HOURS = 1440

FETCH_STATES = ["Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa","Gujarat","Haryana","Himachal Pradesh","Jharkhand","Karnataka","Kerala","Madhya Pradesh",
"Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh","Uttarakhand","West Bengal",
"Andaman and Nicobar Islands","Chandigarh","Dadra and Nagar Haveli and Daman and Diu","Delhi","Jammu and Kashmir","Ladakh","Lakshadweep","Puducherry"] 

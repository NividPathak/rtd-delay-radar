"""Single source of truth for feed URLs, Unity Catalog names, and paths."""

# RTD GTFS-Realtime feeds. RTD moved these to open-data.rtd-denver.com in Fall 2025.
FEED_BASE_URL = "https://open-data.rtd-denver.com/files/gtfs-rt/rtd"
FEEDS: dict[str, str] = {
    "trip_updates": f"{FEED_BASE_URL}/TripUpdate.pb",
    "vehicle_positions": f"{FEED_BASE_URL}/VehiclePosition.pb",
    "alerts": f"{FEED_BASE_URL}/Alerts.pb",
}

# Unity Catalog
CATALOG = "rtd"
SCHEMAS = ["landing", "bronze", "silver", "gold", "ml"]
LANDING_SCHEMA = "landing"
RAW_VOLUME = "raw"
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{RAW_VOLUME}"

# Collector
POLL_INTERVAL_SECONDS = 60
HTTP_TIMEOUT_SECONDS = 20
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 2.0
LOCAL_RAW_DIR = "data/raw"

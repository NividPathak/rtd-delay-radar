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
USER_AGENT = "rtd-delay-radar/0.1 (+https://github.com/NividPathak/rtd-delay-radar)"

# Static GTFS schedule. RTD changes schedules about three times a year.
STATIC_GTFS_URL = "https://www.rtd-denver.com/files/gtfs/google_transit.zip"
STATIC_GTFS_FILES = [
    "feed_info",
    "routes",
    "trips",
    "stop_times",
    "stops",
    "calendar",
    "calendar_dates",
]
STATIC_VOLUME_PATH = f"{RAW_VOLUME_PATH}/static"

# Streaming checkpoints live in their own volume, apart from raw data.
CHECKPOINT_VOLUME = "checkpoints"
CHECKPOINT_VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{CHECKPOINT_VOLUME}"

# The dev bundle target writes to dev_bronze, dev_silver, dev_gold so it never
# touches the tables the scheduled prod job maintains.
DEV_SCHEMA_PREFIX = "dev_"
DEV_SCHEMAS = [f"{DEV_SCHEMA_PREFIX}{s}" for s in ["bronze", "silver", "gold"]]

# Data quality bounds used by silver expectations.
DENVER_BBOX = {"min_lat": 39.3, "max_lat": 40.4, "min_lon": -105.5, "max_lon": -104.4}
MAX_ABS_DELAY_SECONDS = 2 * 60 * 60
LOCAL_TIMEZONE = "America/Denver"


def table_name(schema: str, table: str, schema_prefix: str = "") -> str:
    """Full Unity Catalog name. table_name("bronze", "alerts", "dev_") is rtd.dev_bronze.alerts."""
    return f"{CATALOG}.{schema_prefix}{schema}.{table}"


# Gold labels. The last prediction before a stop drops out of the feed is the observed
# arrival, but only if that prediction was made at most this many seconds before arrival.
LABEL_MAX_LEAD_SECONDS = 120
LATE_THRESHOLD_SECONDS = 5 * 60

# ML problem setup.
HORIZONS = [1, 5, 10, 20]  # stops ahead
TREND_STOPS = 3
HIST_DAYS = 7
TEST_DAYS = 7
MIN_DAYS_FOR_RESULTS = 21
# US federal holidays in the collection window (no extra dependency for a short list).
HOLIDAYS = ["2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25", "2027-01-01"]
MODEL_NAME = f"{CATALOG}.ml.delay_model"

# Live scoring: a trip counts as active if its latest observed stop is this recent.
LIVE_MAX_ANCHOR_AGE_S = 30 * 60
LIVE_VEHICLE_MAX_AGE_S = 15 * 60
MONITORING_DAYS = 30

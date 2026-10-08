"""Explicit Spark schemas for every table. Production code never infers schemas."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---- Parsed protobuf records (what the bronze parser UDF returns per entity) ----

STOP_TIME_UPDATE = StructType(
    [
        StructField("stop_sequence", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("arrival_time", LongType()),
        StructField("arrival_delay", IntegerType()),
        StructField("departure_time", LongType()),
        StructField("departure_delay", IntegerType()),
        StructField("schedule_relationship", StringType()),
    ]
)

TRIP_UPDATE_RECORD = StructType(
    [
        StructField("feed_timestamp", LongType()),
        StructField("entity_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
        StructField("trip_schedule_relationship", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_label", StringType()),
        StructField("update_timestamp", LongType()),
        StructField("trip_delay", IntegerType()),
        StructField("stop_time_updates", ArrayType(STOP_TIME_UPDATE)),
    ]
)

VEHICLE_POSITION_RECORD = StructType(
    [
        StructField("feed_timestamp", LongType()),
        StructField("entity_id", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_label", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", FloatType()),
        StructField("speed", FloatType()),
        StructField("current_status", StringType()),
        StructField("current_stop_sequence", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("vehicle_timestamp", LongType()),
        StructField("occupancy_status", StringType()),
        StructField("occupancy_percentage", IntegerType()),
    ]
)

ACTIVE_PERIOD = StructType([StructField("start", LongType()), StructField("end", LongType())])

INFORMED_ENTITY = StructType(
    [
        StructField("agency_id", StringType()),
        StructField("route_id", StringType()),
        StructField("route_type", IntegerType()),
        StructField("direction_id", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("trip_id", StringType()),
    ]
)

ALERT_RECORD = StructType(
    [
        StructField("feed_timestamp", LongType()),
        StructField("entity_id", StringType()),
        StructField("cause", StringType()),
        StructField("effect", StringType()),
        StructField("severity_level", StringType()),
        StructField("header_text", StringType()),
        StructField("description_text", StringType()),
        StructField("active_periods", ArrayType(ACTIVE_PERIOD)),
        StructField("informed_entities", ArrayType(INFORMED_ENTITY)),
    ]
)

RECORD_SCHEMAS: dict[str, StructType] = {
    "trip_updates": TRIP_UPDATE_RECORD,
    "vehicle_positions": VEHICLE_POSITION_RECORD,
    "alerts": ALERT_RECORD,
}

# ---- Static GTFS files ----
# Read with header=true and no inference, so every column is a string. Spark maps a
# CSV schema by position, not name, so we select these columns by name instead.

STATIC_COLUMNS: dict[str, list[str]] = {
    "feed_info": ["feed_publisher_name", "feed_start_date", "feed_end_date", "feed_version"],
    "routes": ["route_id", "route_short_name", "route_long_name", "route_type"],
    "trips": ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station"],
    "calendar": [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ],
    "calendar_dates": ["service_id", "date", "exception_type"],
}

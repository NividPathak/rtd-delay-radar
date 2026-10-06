"""Decode raw GTFS-Realtime protobuf files with Spark's built-in `from_protobuf`.

Decoding runs inside the JVM, so a large daytime snapshot (20,000+ stop updates)
never has to fit in a Python worker. The protobuf schema comes from the
gtfs-realtime-bindings package, so no .proto or .desc file has to be maintained.
Output columns match the record schemas in `src/common/schemas.py`.
"""

from google.protobuf import descriptor_pb2
from google.transit import gtfs_realtime_pb2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf

from src.common.schemas import RECORD_SCHEMAS

FEED_MESSAGE = "transit_realtime.FeedMessage"

# Which entity field holds each feed's data.
ENTITY_FIELD = {
    "trip_updates": "trip_update",
    "vehicle_positions": "vehicle",
    "alerts": "alert",
}

# One SQL expression per bronze column, reading from `e` (one feed entity).
COLUMNS: dict[str, dict[str, str]] = {
    "trip_updates": {
        "entity_id": "e.id",
        "trip_id": "e.trip_update.trip.trip_id",
        "route_id": "e.trip_update.trip.route_id",
        "direction_id": "e.trip_update.trip.direction_id",
        "start_date": "e.trip_update.trip.start_date",
        "start_time": "e.trip_update.trip.start_time",
        "trip_schedule_relationship": "e.trip_update.trip.schedule_relationship",
        "vehicle_id": "e.trip_update.vehicle.id",
        "vehicle_label": "e.trip_update.vehicle.label",
        "update_timestamp": "e.trip_update.timestamp",
        "trip_delay": "e.trip_update.delay",
        "stop_time_updates": """transform(e.trip_update.stop_time_update, s -> struct(
            s.stop_sequence, s.stop_id, s.arrival.time, s.arrival.delay,
            s.departure.time, s.departure.delay, s.schedule_relationship))""",
    },
    "vehicle_positions": {
        "entity_id": "e.id",
        "vehicle_id": "e.vehicle.vehicle.id",
        "vehicle_label": "e.vehicle.vehicle.label",
        "trip_id": "e.vehicle.trip.trip_id",
        "route_id": "e.vehicle.trip.route_id",
        "direction_id": "e.vehicle.trip.direction_id",
        "start_date": "e.vehicle.trip.start_date",
        "latitude": "e.vehicle.position.latitude",
        "longitude": "e.vehicle.position.longitude",
        "bearing": "e.vehicle.position.bearing",
        "speed": "e.vehicle.position.speed",
        "current_status": "e.vehicle.current_status",
        "current_stop_sequence": "e.vehicle.current_stop_sequence",
        "stop_id": "e.vehicle.stop_id",
        "vehicle_timestamp": "e.vehicle.timestamp",
        "occupancy_status": "e.vehicle.occupancy_status",
        "occupancy_percentage": "e.vehicle.occupancy_percentage",
    },
    "alerts": {
        "entity_id": "e.id",
        "cause": "e.alert.cause",
        "effect": "e.alert.effect",
        "severity_level": "e.alert.severity_level",
        "header_text": "e.alert.header_text.translation[0].text",
        "description_text": "e.alert.description_text.translation[0].text",
        "active_periods": "transform(e.alert.active_period, p -> struct(p.start, p.`end`))",
        "informed_entities": """transform(e.alert.informed_entity, i -> struct(
            i.agency_id, i.route_id, i.route_type, i.direction_id, i.stop_id, i.trip.trip_id))""",
    },
}


def gtfs_descriptor_set() -> bytes:
    """The GTFS-Realtime protobuf schema as a serialized FileDescriptorSet."""
    file_proto = descriptor_pb2.FileDescriptorProto()
    gtfs_realtime_pb2.DESCRIPTOR.CopyToProto(file_proto)
    return descriptor_pb2.FileDescriptorSet(file=[file_proto]).SerializeToString()


def decode_entities(files: DataFrame, feed_name: str) -> DataFrame:
    """binaryFile rows (path, content) to one row per entity of this feed: (source_file, ts, e)."""
    message = from_protobuf("content", FEED_MESSAGE, binaryDescriptorSet=gtfs_descriptor_set())
    entity_field = ENTITY_FIELD[feed_name]
    return (
        files.select(F.col("path").alias("source_file"), message.alias("m"))
        .select(
            "source_file", F.col("m.header.timestamp").alias("ts"), F.explode("m.entity").alias("e")
        )
        .filter(F.col(f"e.{entity_field}").isNotNull())
    )


def to_bronze_records(entities: DataFrame, feed_name: str) -> DataFrame:
    """Select and cast each bronze column to its explicit schema type."""
    schema = RECORD_SCHEMAS[feed_name]
    expressions = {"feed_timestamp": "ts", **COLUMNS[feed_name]}
    return entities.select(
        *[F.expr(expressions[f.name]).cast(f.dataType).alias(f.name) for f in schema.fields],
        "source_file",
    )

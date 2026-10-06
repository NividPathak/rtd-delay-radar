"""Turn one GTFS-Realtime protobuf snapshot into plain Python records.

Each function returns a list of dicts whose keys match the record schemas in
`src/common/schemas.py`. No Spark here, so the parsing is easy to unit test.
A field that is absent in the protobuf becomes None, never a default 0 or "".
"""

from typing import Any

from google.protobuf.message import Message
from google.transit import gtfs_realtime_pb2 as gtfs

Record = dict[str, Any]


def opt(message: Message, field: str) -> Any:
    """Return the field's value if it is set, otherwise None."""
    return getattr(message, field) if message.HasField(field) else None


def opt_enum(message: Message, field: str) -> str | None:
    """Return an enum field's name (e.g. "SKIPPED") if it is set, otherwise None."""
    if not message.HasField(field):
        return None
    enum_type = message.DESCRIPTOR.fields_by_name[field].enum_type
    return enum_type.values_by_number[getattr(message, field)].name


def first_translation(text: Message) -> str | None:
    """Return the first translation of a TranslatedString, or None."""
    return text.translation[0].text if text.translation else None


def read_feed(body: bytes) -> gtfs.FeedMessage:
    """Parse raw bytes into a FeedMessage."""
    feed = gtfs.FeedMessage()
    feed.ParseFromString(body)
    return feed


def stop_time_update_record(stu: Message) -> Record:
    return {
        "stop_sequence": opt(stu, "stop_sequence"),
        "stop_id": opt(stu, "stop_id"),
        "arrival_time": opt(stu.arrival, "time") if stu.HasField("arrival") else None,
        "arrival_delay": opt(stu.arrival, "delay") if stu.HasField("arrival") else None,
        "departure_time": opt(stu.departure, "time") if stu.HasField("departure") else None,
        "departure_delay": opt(stu.departure, "delay") if stu.HasField("departure") else None,
        "schedule_relationship": opt_enum(stu, "schedule_relationship"),
    }


def trip_update_record(feed_ts: int, entity: Message) -> Record:
    tu = entity.trip_update
    return {
        "feed_timestamp": feed_ts,
        "entity_id": entity.id,
        "trip_id": opt(tu.trip, "trip_id"),
        "route_id": opt(tu.trip, "route_id"),
        "direction_id": opt(tu.trip, "direction_id"),
        "start_date": opt(tu.trip, "start_date"),
        "start_time": opt(tu.trip, "start_time"),
        "trip_schedule_relationship": opt_enum(tu.trip, "schedule_relationship"),
        "vehicle_id": opt(tu.vehicle, "id"),
        "vehicle_label": opt(tu.vehicle, "label"),
        "update_timestamp": opt(tu, "timestamp"),
        "trip_delay": opt(tu, "delay"),
        "stop_time_updates": [stop_time_update_record(s) for s in tu.stop_time_update],
    }


def vehicle_position_record(feed_ts: int, entity: Message) -> Record:
    vp = entity.vehicle
    return {
        "feed_timestamp": feed_ts,
        "entity_id": entity.id,
        "vehicle_id": opt(vp.vehicle, "id"),
        "vehicle_label": opt(vp.vehicle, "label"),
        "trip_id": opt(vp.trip, "trip_id"),
        "route_id": opt(vp.trip, "route_id"),
        "direction_id": opt(vp.trip, "direction_id"),
        "start_date": opt(vp.trip, "start_date"),
        "latitude": opt(vp.position, "latitude"),
        "longitude": opt(vp.position, "longitude"),
        "bearing": opt(vp.position, "bearing"),
        "speed": opt(vp.position, "speed"),
        "current_status": opt_enum(vp, "current_status"),
        "current_stop_sequence": opt(vp, "current_stop_sequence"),
        "stop_id": opt(vp, "stop_id"),
        "vehicle_timestamp": opt(vp, "timestamp"),
        "occupancy_status": opt_enum(vp, "occupancy_status"),
        "occupancy_percentage": opt(vp, "occupancy_percentage"),
    }


def informed_entity_record(ie: Message) -> Record:
    return {
        "agency_id": opt(ie, "agency_id"),
        "route_id": opt(ie, "route_id"),
        "route_type": opt(ie, "route_type"),
        "direction_id": opt(ie, "direction_id"),
        "stop_id": opt(ie, "stop_id"),
        "trip_id": opt(ie.trip, "trip_id") if ie.HasField("trip") else None,
    }


def alert_record(feed_ts: int, entity: Message) -> Record:
    alert = entity.alert
    return {
        "feed_timestamp": feed_ts,
        "entity_id": entity.id,
        "cause": opt_enum(alert, "cause"),
        "effect": opt_enum(alert, "effect"),
        "severity_level": opt_enum(alert, "severity_level"),
        "header_text": first_translation(alert.header_text),
        "description_text": first_translation(alert.description_text),
        "active_periods": [
            {"start": opt(p, "start"), "end": opt(p, "end")} for p in alert.active_period
        ],
        "informed_entities": [informed_entity_record(ie) for ie in alert.informed_entity],
    }


ENTITY_PARSERS = {
    "trip_updates": ("trip_update", trip_update_record),
    "vehicle_positions": ("vehicle", vehicle_position_record),
    "alerts": ("alert", alert_record),
}


def parse_snapshot(feed_name: str, body: bytes) -> list[Record]:
    """Parse one snapshot of `feed_name` into one record per entity of that feed's type."""
    entity_field, to_record = ENTITY_PARSERS[feed_name]
    feed = read_feed(body)
    feed_ts = feed.header.timestamp
    return [to_record(feed_ts, e) for e in feed.entity if e.HasField(entity_field)]

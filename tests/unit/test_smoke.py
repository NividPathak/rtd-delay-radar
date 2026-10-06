from google.transit import gtfs_realtime_pb2


def test_gtfs_bindings_import() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    assert feed.header.gtfs_realtime_version == ""

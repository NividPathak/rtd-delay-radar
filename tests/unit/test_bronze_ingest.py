from pathlib import Path

from src.bronze.ingest import parse_binary_files, source_path

FIXTURES = Path(__file__).parent.parent / "fixtures"


def binary_files(spark, *names: str):
    """A DataFrame shaped like Auto Loader's binaryFile output (path, content)."""
    rows = [(f"/Volumes/x/{n}.pb", (FIXTURES / f"{n}.pb").read_bytes()) for n in names]
    return spark.createDataFrame(rows, "path string, content binary")


def test_trip_updates_one_row_per_entity(spark) -> None:
    df = parse_binary_files(binary_files(spark, "trip_updates"), "trip_updates")
    assert df.count() == 19
    assert {"trip_id", "stop_time_updates", "source_file", "ingest_ts"} <= set(df.columns)
    row = df.filter("trip_id = '116034942'").first()
    assert row.route_id == "104L"
    assert row.source_file == "/Volumes/x/trip_updates.pb"
    assert row.stop_time_updates[0].arrival_time == 1791280560


def test_vehicle_positions_keep_null_trip_ids(spark) -> None:
    df = parse_binary_files(binary_files(spark, "vehicle_positions"), "vehicle_positions")
    assert df.count() == 95
    assert df.filter("trip_id is null").count() == 88


def test_alerts_nested_arrays(spark) -> None:
    df = parse_binary_files(binary_files(spark, "alerts"), "alerts")
    assert df.count() == 129
    assert df.selectExpr("size(informed_entities) as n").first().n >= 1


def test_source_path_with_and_without_date() -> None:
    assert source_path("alerts", None).endswith("/raw/feed=alerts/")
    assert source_path("alerts", "2026-10-06").endswith("/raw/feed=alerts/date=2026-10-06/")


def test_spark_decode_matches_python_reference_parser(spark) -> None:
    from src.bronze.parse import parse_snapshot

    for feed in ["trip_updates", "vehicle_positions", "alerts"]:
        spark_rows = parse_binary_files(binary_files(spark, feed), feed).drop(
            "source_file", "ingest_ts"
        )
        actual = [r.asDict(recursive=True) for r in spark_rows.collect()]
        expected = parse_snapshot(feed, (FIXTURES / f"{feed}.pb").read_bytes())
        assert len(actual) == len(expected), feed
        for got, want in zip(actual, expected, strict=True):
            assert got == want, (feed, want["entity_id"])

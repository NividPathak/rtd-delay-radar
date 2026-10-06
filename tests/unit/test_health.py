from pathlib import Path

from src.collector.health import feed_health, find_gaps
from src.collector.storage import save_local, snapshot_relative_path


def test_find_gaps_flags_only_long_gaps() -> None:
    stamps = [0, 60, 120, 600, 660]
    assert find_gaps(stamps, threshold=300) == [(120, 600)]


def test_find_gaps_handles_unsorted_and_empty() -> None:
    assert find_gaps([660, 0, 60]) == [(60, 660)]
    assert find_gaps([]) == []


def test_feed_health_reads_saved_snapshots(tmp_path: Path) -> None:
    for ts in [1791277000, 1791277060, 1791277900]:
        save_local(tmp_path, snapshot_relative_path("alerts", ts), b"x")
    health = feed_health(tmp_path, "alerts")
    assert health.snapshots == 3
    assert health.first_ts == 1791277000
    assert health.last_ts == 1791277900
    assert health.gaps == [(1791277060, 1791277900)]


def test_feed_health_with_no_data(tmp_path: Path) -> None:
    health = feed_health(tmp_path, "alerts")
    assert health.snapshots == 0
    assert health.first_ts is None

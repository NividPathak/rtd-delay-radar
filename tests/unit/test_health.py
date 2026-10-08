from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.collector.health import (
    find_gaps,
    local_file_names,
    summarize,
    timestamps_from_names,
    volume_file_names,
)
from src.collector.storage import save_local, snapshot_relative_path


def test_find_gaps_flags_only_long_gaps() -> None:
    stamps = [0, 60, 120, 600, 660]
    assert find_gaps(stamps, threshold=300) == [(120, 600)]


def test_find_gaps_handles_unsorted_and_empty() -> None:
    assert find_gaps([660, 0, 60]) == [(60, 660)]
    assert find_gaps([]) == []


def test_timestamps_from_names_ignores_other_files() -> None:
    names = ["trip_updates_100.pb", "trip_updates_160.pb", "notes.txt", "alerts_5.pb"]
    assert timestamps_from_names(names, "trip_updates") == [100, 160]


def test_summarize_local_snapshots(tmp_path: Path) -> None:
    for ts in [1791277000, 1791277060, 1791277900]:
        save_local(tmp_path, snapshot_relative_path("alerts", ts), b"x")
    stamps = timestamps_from_names(local_file_names(tmp_path, "alerts"), "alerts")
    health = summarize("alerts", stamps)
    assert health.snapshots == 3
    assert health.first_ts == 1791277000
    assert health.last_ts == 1791277900
    assert health.gaps == [(1791277060, 1791277900)]


def test_summarize_with_no_data() -> None:
    health = summarize("alerts", [])
    assert health.snapshots == 0
    assert health.first_ts is None


def test_volume_file_names_walks_hour_folders_and_skips_missing_dates() -> None:
    def listing(path: str):
        if path.endswith("hour=08"):
            return [SimpleNamespace(name="alerts_1.pb", path=f"{path}/alerts_1.pb")]
        if "date=" in path and "hour=" not in path and path.endswith(_today()):
            return [SimpleNamespace(name="hour=08", path=f"{path}/hour=08")]
        raise FileNotFoundError(path)

    client = MagicMock()
    client.files.list_directory_contents.side_effect = listing
    assert volume_file_names(client, "alerts", days=2) == ["alerts_1.pb"]


def _today() -> str:
    from datetime import UTC, datetime

    return f"{datetime.now(tz=UTC):%Y-%m-%d}"

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.collector.freshness import is_fresh, latest_volume_timestamp, recent_hour_dirs

NOW = datetime(2026, 10, 7, 0, 10, tzinfo=UTC)


def test_recent_hour_dirs_cross_midnight() -> None:
    dirs = recent_hour_dirs("alerts", NOW)
    assert dirs[0].endswith("feed=alerts/date=2026-10-06/hour=23")
    assert dirs[1].endswith("feed=alerts/date=2026-10-07/hour=00")


def test_latest_volume_timestamp_uses_both_hours_and_skips_missing() -> None:
    def listing(path: str):
        if path.endswith("hour=23"):
            return [SimpleNamespace(name="alerts_100.pb"), SimpleNamespace(name="alerts_200.pb")]
        raise FileNotFoundError(path)

    client = MagicMock()
    client.files.list_directory_contents.side_effect = listing
    assert latest_volume_timestamp(client, "alerts", NOW) == 200


def test_is_fresh() -> None:
    now_ts = int(NOW.timestamp())
    assert is_fresh(now_ts - 60, NOW, 300)
    assert not is_fresh(now_ts - 301, NOW, 300)
    assert not is_fresh(None, NOW, 300)

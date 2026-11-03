from datetime import date
from pathlib import Path

from src.collector.upload_backlog import local_snapshots_for_date, missing_from_volume
from src.collector.uploader import VolumeUploader


def test_only_snapshots_absent_from_the_volume_are_uploaded(tmp_path: Path) -> None:
    hour = tmp_path / "feed=alerts" / "date=2026-10-20" / "hour=05"
    hour.mkdir(parents=True)
    for ts in (3, 1, 2):
        (hour / f"alerts_{ts}.pb").write_bytes(b"x")
    local = local_snapshots_for_date(tmp_path, "alerts", date(2026, 10, 20))
    missing = missing_from_volume(local, ["alerts_2.pb"])
    assert missing == [
        "feed=alerts/date=2026-10-20/hour=05/alerts_1.pb",
        "feed=alerts/date=2026-10-20/hour=05/alerts_3.pb",
    ]


def test_missing_local_date_is_empty(tmp_path: Path) -> None:
    assert local_snapshots_for_date(tmp_path, "alerts", date(2026, 10, 20)) == {}


def test_uploader_does_not_log_in_at_startup() -> None:
    uploader = VolumeUploader()  # an expired login must not stop the collector here
    assert uploader._client is None

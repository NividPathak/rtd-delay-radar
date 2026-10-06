from pathlib import Path
from unittest.mock import MagicMock

from src.collector.storage import save_local, snapshot_relative_path
from src.collector.uploader import VolumeUploader


def test_snapshot_path_is_partitioned_in_utc() -> None:
    # 2026-10-06 04:54:37 UTC (22:54 the previous day in Denver)
    path = snapshot_relative_path("trip_updates", 1791276877)
    assert path == "feed=trip_updates/date=2026-10-06/hour=04/trip_updates_1791276877.pb"


def test_save_local_creates_folders(tmp_path: Path) -> None:
    target = save_local(tmp_path, "feed=a/date=2026-01-01/hour=00/a_1.pb", b"x")
    assert target.read_bytes() == b"x"


def test_uploader_writes_under_volume_root() -> None:
    client = MagicMock()
    uploader = VolumeUploader(client=client, root="/Volumes/rtd/landing/raw")
    full_path = uploader.upload("feed=a/x.pb", b"data")
    assert full_path == "/Volumes/rtd/landing/raw/feed=a/x.pb"
    args, kwargs = client.files.upload.call_args
    assert args[0] == full_path
    assert args[1].read() == b"data"
    assert kwargs == {"overwrite": True}

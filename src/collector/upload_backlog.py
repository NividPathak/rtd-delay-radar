"""Upload snapshots that were saved locally but never reached the volume.

The collector queues failed uploads in memory only, so a restart forgets them. This
compares local files with the volume, date by date, and uploads the missing ones.
They are real snapshots collected at the time, not reconstructed data.

Usage:
    python -m src.collector.upload_backlog --start 2026-10-13 --end 2026-10-28
    python -m src.collector.upload_backlog --start 2026-10-13 --end 2026-10-28 --dry-run
    python -m src.collector.upload_backlog --start 2026-10-13 --end 2026-10-28 --skip-check

--skip-check uploads every local snapshot without listing the volume first. Listing very
busy hour folders can time out. Re-uploading a file that is already there writes the
same bytes to the same path, and Auto Loader skips paths it has already ingested.
"""

import argparse
from datetime import date, timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient

from src.collector.health import volume_names_for_date
from src.collector.uploader import VolumeUploader
from src.common import config


def local_snapshots_for_date(local_root: Path, feed: str, day: date) -> dict[str, str]:
    """Map file name to relative path for one feed's local snapshots on one UTC date."""
    date_dir = local_root / f"feed={feed}" / f"date={day:%Y-%m-%d}"
    return {f.name: str(f.relative_to(local_root)) for f in date_dir.rglob("*.pb")}


def missing_from_volume(local: dict[str, str], on_volume: list[str]) -> list[str]:
    """Relative paths of local snapshots whose file name is not on the volume, oldest first."""
    present = set(on_volume)
    return [path for name, path in sorted(local.items()) if name not in present]


def dates_between(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="UTC date")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="UTC date")
    parser.add_argument("--dry-run", action="store_true", help="count only, upload nothing")
    parser.add_argument("--skip-check", action="store_true", help="upload all, do not list")
    args = parser.parse_args()

    local_root = Path(config.LOCAL_RAW_DIR)
    uploader = VolumeUploader()
    lister = WorkspaceClient()  # default timeouts: listing a busy day takes longer than an upload
    total = 0
    for day in dates_between(args.start, args.end):
        for feed in config.FEEDS:
            local = local_snapshots_for_date(local_root, feed, day)
            on_volume = [] if args.skip_check else volume_names_for_date(lister, feed, day)
            missing = missing_from_volume(local, on_volume)
            print(f"{day} {feed}: {len(local)} local, {len(missing)} missing", flush=True)
            total += len(missing)
            if args.dry_run:
                continue
            for relative_path in missing:
                uploader.upload(relative_path, (local_root / relative_path).read_bytes())
    print(f"{'would upload' if args.dry_run else 'uploaded'} {total} snapshots")


if __name__ == "__main__":
    main()

"""Report the collected date range and any gaps longer than 5 minutes, per feed.

Usage:
    python -m src.collector.health                    # local files on this machine
    python -m src.collector.health --volume --days 2  # files in the Unity Catalog volume
"""

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient

from src.common import config

GAP_THRESHOLD_SECONDS = 300


@dataclass
class FeedHealth:
    """Summary of one feed's collected snapshots."""

    feed: str
    snapshots: int
    first_ts: int | None
    last_ts: int | None
    gaps: list[tuple[int, int]]


def find_gaps(
    timestamps: list[int], threshold: int = GAP_THRESHOLD_SECONDS
) -> list[tuple[int, int]]:
    """Return (start, end) pairs where consecutive snapshots are more than `threshold` apart."""
    ordered = sorted(timestamps)
    return [(a, b) for a, b in zip(ordered, ordered[1:], strict=False) if b - a > threshold]


def timestamps_from_names(names: Iterable[str], feed: str) -> list[int]:
    """Read header timestamps from snapshot file names like `<feed>_<ts>.pb`."""
    prefix = f"{feed}_"
    return [
        int(name[len(prefix) : -len(".pb")])
        for name in names
        if name.startswith(prefix) and name.endswith(".pb")
    ]


def summarize(feed: str, stamps: list[int]) -> FeedHealth:
    """Summarize snapshot count, date range, and gaps for one feed."""
    return FeedHealth(
        feed=feed,
        snapshots=len(stamps),
        first_ts=min(stamps, default=None),
        last_ts=max(stamps, default=None),
        gaps=find_gaps(stamps),
    )


def local_file_names(local_root: Path, feed: str) -> list[str]:
    """List snapshot file names saved locally for one feed."""
    return [f.name for f in (local_root / f"feed={feed}").rglob("*.pb")]


def volume_file_names(client: WorkspaceClient, feed: str, days: int) -> list[str]:
    """List snapshot file names in the volume for one feed over the last `days` UTC dates."""
    today = datetime.now(tz=UTC).date()
    names: list[str] = []
    for offset in range(days - 1, -1, -1):
        day = today - timedelta(days=offset)
        date_dir = f"{config.RAW_VOLUME_PATH}/feed={feed}/date={day:%Y-%m-%d}"
        try:
            hour_dirs = [e.path for e in client.files.list_directory_contents(date_dir)]
        except Exception:  # the date folder does not exist yet
            continue
        for hour_dir in hour_dirs:
            names += [e.name for e in client.files.list_directory_contents(hour_dir)]
    return names


def fmt(ts: int | None) -> str:
    if ts is None:
        return "none"
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def print_report(health: FeedHealth) -> None:
    span = f"{fmt(health.first_ts)} to {fmt(health.last_ts)}"
    print(f"{health.feed}: {health.snapshots} snapshots, {span}, {len(health.gaps)} gaps")
    for start, end in health.gaps:
        print(f"  gap {(end - start) / 60:.1f} min: {fmt(start)} to {fmt(end)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--volume", action="store_true", help="read the UC volume, not local")
    parser.add_argument("--days", type=int, default=3, help="UTC dates to scan with --volume")
    args = parser.parse_args()

    client = None
    if args.volume:
        client = WorkspaceClient()
    for feed in config.FEEDS:
        if client is None:
            names = local_file_names(Path(config.LOCAL_RAW_DIR), feed)
        else:
            names = volume_file_names(client, feed, args.days)
        print_report(summarize(feed, timestamps_from_names(names, feed)))


if __name__ == "__main__":
    main()

"""Check whether the volume has a recent snapshot. Used by the GitHub Actions backup runner.

Exit code 0 means fresh (another collector is running), 1 means stale (start collecting).

Usage:
    python -m src.collector.freshness --max-age 300
"""

import argparse
import sys
from datetime import UTC, datetime, timedelta

from databricks.sdk import WorkspaceClient

from src.collector.health import timestamps_from_names
from src.common import config

CHECK_FEED = "trip_updates"


def recent_hour_dirs(feed: str, now: datetime) -> list[str]:
    """Volume folders for the current and previous UTC hour."""
    hours = [now - timedelta(hours=1), now]
    return [
        f"{config.RAW_VOLUME_PATH}/feed={feed}/date={h:%Y-%m-%d}/hour={h:%H}" for h in hours
    ]


def latest_volume_timestamp(client: WorkspaceClient, feed: str, now: datetime) -> int | None:
    """Newest snapshot timestamp for `feed` in the last two hour folders, or None."""
    names: list[str] = []
    for hour_dir in recent_hour_dirs(feed, now):
        try:
            names += [e.name for e in client.files.list_directory_contents(hour_dir)]
        except Exception:  # the hour folder does not exist yet
            continue
    return max(timestamps_from_names(names, feed), default=None)


def is_fresh(latest_ts: int | None, now: datetime, max_age_seconds: int) -> bool:
    """True when the newest snapshot is at most `max_age_seconds` old."""
    return latest_ts is not None and now.timestamp() - latest_ts <= max_age_seconds


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--max-age", type=int, default=300, help="seconds before stale")
    args = parser.parse_args()

    now = datetime.now(tz=UTC)
    latest = latest_volume_timestamp(WorkspaceClient(), CHECK_FEED, now)
    fresh = is_fresh(latest, now, args.max_age)
    age = "none" if latest is None else f"{now.timestamp() - latest:.0f}s"
    print(f"latest {CHECK_FEED} snapshot age: {age} -> {'fresh' if fresh else 'stale'}")
    sys.exit(0 if fresh else 1)


if __name__ == "__main__":
    main()

"""Report the collected date range and any gaps longer than 5 minutes, per feed.

Usage:
    python -m src.collector.health
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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


def snapshot_timestamps(local_root: Path, feed: str) -> list[int]:
    """Read header timestamps from the file names saved for one feed."""
    files = (local_root / f"feed={feed}").rglob(f"{feed}_*.pb")
    return [int(f.stem.rsplit("_", 1)[1]) for f in files]


def feed_health(local_root: Path, feed: str) -> FeedHealth:
    """Summarize snapshot count, date range, and gaps for one feed."""
    stamps = snapshot_timestamps(local_root, feed)
    return FeedHealth(
        feed=feed,
        snapshots=len(stamps),
        first_ts=min(stamps, default=None),
        last_ts=max(stamps, default=None),
        gaps=find_gaps(stamps),
    )


def fmt(ts: int | None) -> str:
    if ts is None:
        return "none"
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    local_root = Path(config.LOCAL_RAW_DIR)
    for feed in config.FEEDS:
        health = feed_health(local_root, feed)
        span = f"{fmt(health.first_ts)} to {fmt(health.last_ts)}"
        print(f"{feed}: {health.snapshots} snapshots, {span}")
        for start, end in health.gaps:
            print(f"  gap {(end - start) / 60:.1f} min: {fmt(start)} to {fmt(end)}")


if __name__ == "__main__":
    main()

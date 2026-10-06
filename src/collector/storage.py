"""Build partitioned paths for raw feed snapshots and save them locally."""

from datetime import UTC, datetime
from pathlib import Path


def snapshot_relative_path(feed_name: str, header_ts: int) -> str:
    """Return `feed=<name>/date=YYYY-MM-DD/hour=HH/<name>_<ts>.pb`, partitioned in UTC."""
    moment = datetime.fromtimestamp(header_ts, tz=UTC)
    return (
        f"feed={feed_name}/date={moment:%Y-%m-%d}/hour={moment:%H}/"
        f"{feed_name}_{header_ts}.pb"
    )


def save_local(root: Path, relative_path: str, body: bytes) -> Path:
    """Write `body` under `root/relative_path`, creating folders as needed."""
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    return target

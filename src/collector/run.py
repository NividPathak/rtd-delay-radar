"""Poll RTD feeds every minute, save new snapshots locally, and upload them to the volume.

Usage:
    python -m src.collector.run            # run forever
    python -m src.collector.run --once     # one poll cycle, then exit
    python -m src.collector.run --no-upload
"""

import argparse
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from src.collector.fetch import fetch_with_retries, header_timestamp
from src.collector.storage import save_local, snapshot_relative_path
from src.common import config

logger = logging.getLogger("collector")

Uploader = Callable[[str, bytes], object]


@dataclass
class CollectorState:
    """What the collector remembers between cycles."""

    last_seen: dict[str, int] = field(default_factory=dict)
    pending_uploads: list[str] = field(default_factory=list)


def poll_feed(
    name: str,
    url: str,
    state: CollectorState,
    local_root: Path,
    upload: Uploader | None,
    fetch: Callable[[str], bytes] = fetch_with_retries,
) -> str | None:
    """Fetch one feed. Save and upload it if its header timestamp is new.

    Returns the snapshot's relative path, or None if it was a duplicate.
    """
    body = fetch(url)
    header_ts = header_timestamp(body)
    if state.last_seen.get(name) == header_ts:
        logger.info("%s unchanged (ts=%d), skipped", name, header_ts)
        return None
    relative_path = snapshot_relative_path(name, header_ts)
    save_local(local_root, relative_path, body)
    state.last_seen[name] = header_ts
    if upload is not None:
        try_upload(relative_path, local_root, state, upload)
    logger.info("%s saved %s (%d bytes)", name, relative_path, len(body))
    return relative_path


def try_upload(
    relative_path: str, local_root: Path, state: CollectorState, upload: Uploader
) -> bool:
    """Upload one saved snapshot. On failure, queue it for the next cycle."""
    try:
        upload(relative_path, (local_root / relative_path).read_bytes())
        return True
    except Exception as error:
        logger.error("upload failed, queued for retry: %s (%s)", relative_path, error)
        if relative_path not in state.pending_uploads:
            state.pending_uploads.append(relative_path)
        return False


def retry_pending(state: CollectorState, local_root: Path, upload: Uploader) -> None:
    """Retry uploads that failed in earlier cycles."""
    waiting = list(state.pending_uploads)
    state.pending_uploads.clear()
    for relative_path in waiting:
        try_upload(relative_path, local_root, state, upload)


def poll_once(
    state: CollectorState,
    local_root: Path,
    upload: Uploader | None,
    feeds: dict[str, str] = config.FEEDS,
    fetch: Callable[[str], bytes] = fetch_with_retries,
) -> None:
    """Run one cycle over all feeds. An error in one feed never stops the others."""
    if upload is not None:
        retry_pending(state, local_root, upload)
    for name, url in feeds.items():
        try:
            poll_feed(name, url, state, local_root, upload, fetch)
        except Exception:
            logger.exception("poll failed for %s", name)


def run_forever(state: CollectorState, local_root: Path, upload: Uploader | None) -> None:
    """Poll on a fixed interval until the process is stopped."""
    while True:
        started = time.monotonic()
        poll_once(state, local_root, upload)
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, config.POLL_INTERVAL_SECONDS - elapsed))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--no-upload", action="store_true", help="save locally only")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    upload = None
    if not args.no_upload:
        from src.collector.uploader import VolumeUploader

        upload = VolumeUploader().upload

    state = CollectorState()
    local_root = Path(config.LOCAL_RAW_DIR)
    if args.once:
        poll_once(state, local_root, upload)
    else:
        run_forever(state, local_root, upload)


if __name__ == "__main__":
    main()

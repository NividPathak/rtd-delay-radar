"""Download GTFS-Realtime feeds with retries and read their header timestamp."""

import logging
import time
import urllib.request
from collections.abc import Callable

from google.transit import gtfs_realtime_pb2

from src.common import config

logger = logging.getLogger(__name__)


def http_get(url: str, timeout: float = config.HTTP_TIMEOUT_SECONDS) -> bytes:
    """Return the body of a GET request to `url`."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def fetch_with_retries(
    url: str,
    get: Callable[[str], bytes] = http_get,
    max_retries: int = config.MAX_RETRIES,
    backoff_base: float = config.BACKOFF_BASE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Fetch `url`, retrying with exponential backoff. Raises the last error if all tries fail."""
    for attempt in range(max_retries + 1):
        try:
            return get(url)
        except Exception as error:
            if attempt == max_retries:
                raise
            wait = backoff_base * 2**attempt
            logger.warning("fetch failed, retry %d in %.0fs: %s %s", attempt + 1, wait, url, error)
            sleep(wait)
    raise AssertionError("unreachable")


def header_timestamp(body: bytes) -> int:
    """Parse a FeedMessage and return its header timestamp in epoch seconds."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(body)
    return int(feed.header.timestamp)

from pathlib import Path

import pytest
from google.protobuf.message import DecodeError

from src.collector.fetch import fetch_with_retries, header_timestamp

FIXTURES = Path(__file__).parent.parent / "fixtures"


def no_sleep(_: float) -> None:
    pass


def test_fetch_returns_body_on_first_success() -> None:
    body = fetch_with_retries("u", get=lambda _: b"ok", sleep=no_sleep)
    assert body == b"ok"


def test_fetch_retries_then_succeeds() -> None:
    calls = []
    waits = []

    def flaky(url: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise OSError("boom")
        return b"ok"

    body = fetch_with_retries("u", get=flaky, backoff_base=1.0, sleep=waits.append)
    assert body == b"ok"
    assert waits == [1.0, 2.0]


def test_fetch_raises_after_max_retries() -> None:
    def always_fail(_: str) -> bytes:
        raise OSError("down")

    with pytest.raises(OSError):
        fetch_with_retries("u", get=always_fail, max_retries=2, sleep=no_sleep)


@pytest.mark.parametrize("name", ["trip_updates", "vehicle_positions", "alerts"])
def test_header_timestamp_from_fixture(name: str) -> None:
    body = (FIXTURES / f"{name}.pb").read_bytes()
    assert header_timestamp(body) > 1_700_000_000


def test_header_timestamp_rejects_garbage() -> None:
    with pytest.raises(DecodeError):
        header_timestamp(b"\xff\xff not protobuf")

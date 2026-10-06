from pathlib import Path

from src.collector.run import CollectorState, poll_once

FIXTURES = Path(__file__).parent.parent / "fixtures"
FEEDS = {"trip_updates": "url-tu", "alerts": "url-al"}
BODIES = {
    "url-tu": (FIXTURES / "trip_updates.pb").read_bytes(),
    "url-al": (FIXTURES / "alerts.pb").read_bytes(),
}


def fake_fetch(url: str) -> bytes:
    return BODIES[url]


def saved_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.pb"))


def test_new_snapshots_are_saved_and_uploaded(tmp_path: Path) -> None:
    uploaded: list[str] = []
    poll_once(CollectorState(), tmp_path, lambda p, b: uploaded.append(p), FEEDS, fake_fetch)
    assert len(saved_files(tmp_path)) == 2
    assert len(uploaded) == 2
    assert all(p.startswith("feed=") for p in uploaded)


def test_duplicate_header_timestamp_is_skipped(tmp_path: Path) -> None:
    state = CollectorState()
    uploaded: list[str] = []
    poll_once(state, tmp_path, lambda p, b: uploaded.append(p), FEEDS, fake_fetch)
    poll_once(state, tmp_path, lambda p, b: uploaded.append(p), FEEDS, fake_fetch)
    assert len(uploaded) == 2


def test_one_bad_feed_does_not_stop_the_others(tmp_path: Path) -> None:
    def fetch(url: str) -> bytes:
        if url == "url-tu":
            raise OSError("feed down")
        return BODIES[url]

    poll_once(CollectorState(), tmp_path, None, FEEDS, fetch)
    assert [p.name.split("_")[0] for p in saved_files(tmp_path)] == ["alerts"]


def test_failed_upload_is_retried_next_cycle(tmp_path: Path) -> None:
    state = CollectorState()
    calls: list[str] = []

    def flaky_upload(path: str, body: bytes) -> None:
        calls.append(path)
        if len(calls) <= 2:
            raise OSError("volume unavailable")

    poll_once(state, tmp_path, flaky_upload, FEEDS, fake_fetch)
    assert len(state.pending_uploads) == 2

    poll_once(state, tmp_path, flaky_upload, FEEDS, fake_fetch)
    assert state.pending_uploads == []
    assert len(calls) == 4

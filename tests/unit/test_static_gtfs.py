import io
import zipfile

from src.collector.static_gtfs import extract_files, feed_version


def make_zip(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return buffer.getvalue()


def test_extract_only_requested_files() -> None:
    body = make_zip({"routes.txt": "route_id\n0\n", "shapes.txt": "big"})
    assert extract_files(body, ["routes"]) == {"routes": b"route_id\n0\n"}


def test_feed_version_is_folder_safe() -> None:
    info = "﻿feed_publisher_name,feed_version\nRTD,Sep26-37209 10/11\n".encode()
    assert feed_version(info) == "Sep26-37209_10_11"

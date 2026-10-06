"""Download RTD's static GTFS schedule and upload the files we use to the volume.

Files land in /Volumes/rtd/landing/raw/static/version=<feed_version>/<file>.txt.
A version that is already in the volume is skipped. RTD changes schedules about
three times a year, so running this daily or weekly is plenty.

Usage:
    python -m src.collector.static_gtfs
"""

import csv
import io
import zipfile

from databricks.sdk import WorkspaceClient

from src.collector.fetch import fetch_with_retries
from src.common import config


def extract_files(zip_bytes: bytes, names: list[str]) -> dict[str, bytes]:
    """Return {name: file bytes} for each `<name>.txt` in the zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        return {name: archive.read(f"{name}.txt") for name in names}


def feed_version(feed_info: bytes) -> str:
    """Read `feed_version` from feed_info.txt and make it safe for a folder name."""
    reader = csv.DictReader(io.StringIO(feed_info.decode("utf-8-sig")))
    version = next(reader)["feed_version"]
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in version)


def version_exists(client: WorkspaceClient, version_dir: str) -> bool:
    try:
        client.files.get_directory_metadata(version_dir)
        return True
    except Exception:
        return False


def main() -> None:
    files = extract_files(fetch_with_retries(config.STATIC_GTFS_URL), config.STATIC_GTFS_FILES)
    version = feed_version(files["feed_info"])
    version_dir = f"{config.STATIC_VOLUME_PATH}/version={version}"
    client = WorkspaceClient()
    if version_exists(client, version_dir):
        print(f"static GTFS {version} already in the volume, skipped")
        return
    for name, body in files.items():
        client.files.upload(f"{version_dir}/{name}.txt", io.BytesIO(body), overwrite=True)
        print(f"uploaded {name}.txt ({len(body) / 1e6:.1f} MB)")
    print(f"static GTFS {version} -> {version_dir}")


if __name__ == "__main__":
    main()

"""Upload raw snapshots to the Unity Catalog volume with the Databricks SDK."""

import io

from databricks.sdk import WorkspaceClient

from src.common import config


class VolumeUploader:
    """Uploads bytes to `/Volumes/rtd/landing/raw/<relative_path>`."""

    def __init__(
        self, client: WorkspaceClient | None = None, root: str = config.RAW_VOLUME_PATH
    ) -> None:
        self.client = client or WorkspaceClient()
        self.root = root

    def upload(self, relative_path: str, body: bytes) -> str:
        """Upload `body` and return the full volume path."""
        full_path = f"{self.root}/{relative_path}"
        self.client.files.upload(full_path, io.BytesIO(body), overwrite=True)
        return full_path

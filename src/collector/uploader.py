"""Upload raw snapshots to the Unity Catalog volume with the Databricks SDK."""

import io

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from src.common import config


class VolumeUploader:
    """Uploads bytes to `/Volumes/rtd/landing/raw/<relative_path>`.

    The client is created on first upload, not at startup, so an expired login makes
    uploads fail (and queue) instead of stopping the collector from saving snapshots.
    """

    def __init__(
        self, client: WorkspaceClient | None = None, root: str = config.RAW_VOLUME_PATH
    ) -> None:
        self._client = client
        self.root = root

    @property
    def client(self) -> WorkspaceClient:
        if self._client is None:
            timeouts = Config(
                http_timeout_seconds=config.UPLOAD_TIMEOUT_SECONDS,
                retry_timeout_seconds=config.UPLOAD_TIMEOUT_SECONDS,
            )
            self._client = WorkspaceClient(config=timeouts)
        return self._client

    def upload(self, relative_path: str, body: bytes) -> str:
        """Upload `body` and return the full volume path."""
        full_path = f"{self.root}/{relative_path}"
        self.client.files.upload(full_path, io.BytesIO(body), overwrite=True)
        return full_path

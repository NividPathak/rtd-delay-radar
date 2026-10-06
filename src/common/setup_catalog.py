"""Create the `rtd` catalog, its schemas, and the raw landing volume. Safe to rerun.

Usage:
    python -m src.common.setup_catalog
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import StatementState

from src.common import config

ALREADY_THERE = (AlreadyExists, ResourceAlreadyExists)


def create_catalog(client: WorkspaceClient) -> None:
    """Create the project catalog with SQL on a SQL warehouse.

    The catalogs REST API needs a storage location. In a serverless workspace,
    SQL `CREATE CATALOG` uses Databricks default storage instead.
    """
    warehouse_id = next(iter(client.warehouses.list())).id
    response = client.statement_execution.execute_statement(
        statement=f"CREATE CATALOG IF NOT EXISTS {config.CATALOG} COMMENT 'RTD Delay Radar'",
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )
    state = response.status.state
    if state != StatementState.SUCCEEDED:
        raise RuntimeError(f"CREATE CATALOG ended in {state}: {response.status.error}")
    print(f"catalog {config.CATALOG} ready")


def create_schemas(client: WorkspaceClient) -> None:
    """Create each medallion schema inside the project catalog."""
    for schema in config.SCHEMAS:
        try:
            client.schemas.create(name=schema, catalog_name=config.CATALOG)
            print(f"created schema {config.CATALOG}.{schema}")
        except ALREADY_THERE:
            print(f"schema {config.CATALOG}.{schema} already exists")


def create_raw_volume(client: WorkspaceClient) -> None:
    """Create the managed volume that holds raw feed snapshots."""
    try:
        client.volumes.create(
            catalog_name=config.CATALOG,
            schema_name=config.LANDING_SCHEMA,
            name=config.RAW_VOLUME,
            volume_type=VolumeType.MANAGED,
        )
        print(f"created volume {config.RAW_VOLUME_PATH}")
    except ALREADY_THERE:
        print(f"volume {config.RAW_VOLUME_PATH} already exists")


def main() -> None:
    client = WorkspaceClient()
    create_catalog(client)
    create_schemas(client)
    create_raw_volume(client)


if __name__ == "__main__":
    main()

"""Create the `rtd` catalog, its schemas, and the landing volumes. Safe to rerun.

Usage:
    python -m src.common.setup_catalog
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import StatementState

from src.common import config


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
    """Create each medallion schema, plus the dev_ copies used by the dev bundle target."""
    existing = {s.name for s in client.schemas.list(catalog_name=config.CATALOG)}
    for schema in config.SCHEMAS + config.DEV_SCHEMAS:
        if schema in existing:
            print(f"schema {config.CATALOG}.{schema} already exists")
            continue
        client.schemas.create(name=schema, catalog_name=config.CATALOG)
        print(f"created schema {config.CATALOG}.{schema}")


def create_volume(client: WorkspaceClient, name: str) -> None:
    """Create a managed volume in the landing schema."""
    path = f"/Volumes/{config.CATALOG}/{config.LANDING_SCHEMA}/{name}"
    existing = {
        v.name
        for v in client.volumes.list(catalog_name=config.CATALOG, schema_name=config.LANDING_SCHEMA)
    }
    if name in existing:
        print(f"volume {path} already exists")
        return
    client.volumes.create(
        catalog_name=config.CATALOG,
        schema_name=config.LANDING_SCHEMA,
        name=name,
        volume_type=VolumeType.MANAGED,
    )
    print(f"created volume {path}")


def main() -> None:
    client = WorkspaceClient()
    create_catalog(client)
    create_schemas(client)
    create_volume(client, config.RAW_VOLUME)
    create_volume(client, config.CHECKPOINT_VOLUME)


if __name__ == "__main__":
    main()

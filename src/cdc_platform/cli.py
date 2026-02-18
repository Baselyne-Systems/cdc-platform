"""Typer CLI for the CDC platform."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog
import typer
from confluent_kafka import Message
from rich.console import Console
from rich.table import Table

from cdc_platform.config.loader import load_pipeline_config, load_platform_config
from cdc_platform.config.models import PipelineConfig, PlatformConfig
from cdc_platform.observability.health import (
    Status,
    check_platform_health,
)
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.streaming.consumer import CDCConsumer

logger = structlog.get_logger()
console = Console()
app = typer.Typer(name="cdc", help="CDC Platform CLI")
lakehouse_app = typer.Typer(name="lakehouse", help="Iceberg lakehouse operations")
app.add_typer(lakehouse_app)


def _load(
    config_path: str,
    platform_config: str | None = None,
) -> tuple[PipelineConfig, PlatformConfig]:
    path = Path(config_path)
    if not path.exists():
        console.print(f"[red]Config file not found: {path}[/red]")
        raise typer.Exit(1)
    pipeline = load_pipeline_config(path)
    platform = load_platform_config(Path(platform_config) if platform_config else None)
    return pipeline, platform


@app.command()
def validate(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Validate a pipeline configuration file."""
    try:
        pipeline, platform = _load(config_path, platform_config)
        console.print(f"[green]Valid[/green] — pipeline_id={pipeline.pipeline_id}")
        console.print(
            f"  source: {pipeline.source.source_type} → {pipeline.source.database}"
        )
        console.print(f"  tables: {pipeline.source.tables}")
        console.print(f"  kafka:  {platform.kafka.bootstrap_servers}")
        platform_source = platform_config or "(defaults)"
        console.print(f"  platform config: {platform_source}")
        if pipeline.sinks:
            console.print(f"  sinks:  {len(pipeline.sinks)}")
            for s in pipeline.sinks:
                status = "enabled" if s.enabled else "disabled"
                console.print(f"    - {s.sink_id} ({s.sink_type}) [{status}]")
        else:
            console.print("  sinks:  (none)")
    except Exception as exc:
        console.print(f"[red]Validation error:[/red] {exc}")
        raise typer.Exit(1) from exc


@app.command()
def deploy(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Validate config and register the Debezium connector."""
    pipeline, platform = _load(config_path, platform_config)

    async def _deploy() -> None:
        async with DebeziumClient(platform.connector) as client:
            await client.wait_until_ready()
            result = await client.register_connector(pipeline, platform)
            console.print(
                f"[green]Connector registered:[/green] {result.get('name', 'unknown')}"
            )

    asyncio.run(_deploy())


@app.command()
def health(
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Check health of all platform components."""
    platform = load_platform_config(Path(platform_config) if platform_config else None)
    result = check_platform_health(platform)

    table = Table(title="Platform Health")
    table.add_column("Component", style="cyan")
    table.add_column("Status")
    table.add_column("Detail")

    for c in result.components:
        style = "green" if c.status == Status.HEALTHY else "red"
        table.add_row(c.name, f"[{style}]{c.status}[/{style}]", c.detail)

    console.print(table)
    if not result.healthy:
        raise typer.Exit(1)


@app.command()
def consume(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Start a debug console consumer for CDC events."""
    pipeline, platform = _load(config_path, platform_config)

    from cdc_platform.streaming.topics import topics_for_pipeline

    cdc_topics = [
        t for t in topics_for_pipeline(pipeline, platform) if not t.endswith(".dlq")
    ]

    async def handler(
        key: dict[str, Any] | None, value: dict[str, Any] | None, msg: Message
    ) -> None:
        console.print(
            f"[cyan]{msg.topic()}[/cyan] p={msg.partition()} o={msg.offset()}"
        )
        if key:
            console.print(f"  key:   {key}")
        if value:
            console.print(f"  value: {value}")
        console.print()

    console.print(f"[yellow]Consuming from:[/yellow] {cdc_topics}")
    consumer = CDCConsumer(
        topics=cdc_topics,
        kafka_config=platform.kafka,
        handler=handler,
    )
    asyncio.run(consumer.consume())


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Run the full CDC pipeline (source → Kafka → sinks)."""
    pipeline, platform = _load(config_path, platform_config)

    from cdc_platform.pipeline.runner import Pipeline

    console.print(f"[yellow]Starting pipeline:[/yellow] {pipeline.pipeline_id}")
    if pipeline.sinks:
        for s in pipeline.sinks:
            if s.enabled:
                console.print(f"  sink: {s.sink_id} ({s.sink_type})")
    else:
        console.print("  [dim]No sinks configured — events will be consumed only[/dim]")

    runner = Pipeline(pipeline, platform)
    try:
        runner.start()
    except KeyboardInterrupt:
        runner.stop()


# ---------------------------------------------------------------------------
# Lakehouse sub-commands
# ---------------------------------------------------------------------------


def _load_iceberg_table(
    config_path: str,
    platform_config: str | None,
    sink_id: str | None,
) -> Any:
    """Load a pyiceberg Table object from pipeline config."""
    from pyiceberg.catalog import load_catalog

    pipeline, _platform = _load(config_path, platform_config)

    from cdc_platform.config.models import SinkType

    iceberg_sinks = [
        s
        for s in pipeline.sinks
        if s.sink_type == SinkType.ICEBERG and s.iceberg is not None
    ]
    if not iceberg_sinks:
        console.print("[red]No Iceberg sinks found in pipeline config[/red]")
        raise typer.Exit(1)

    if sink_id:
        matches = [s for s in iceberg_sinks if s.sink_id == sink_id]
        if not matches:
            console.print(f"[red]Sink '{sink_id}' not found[/red]")
            raise typer.Exit(1)
        sink_cfg = matches[0]
    else:
        sink_cfg = iceberg_sinks[0]

    ice = sink_cfg.iceberg
    assert ice is not None
    catalog_props: dict[str, str] = {
        "uri": ice.catalog_uri,
        "warehouse": ice.warehouse,
    }
    if ice.s3_endpoint is not None:
        catalog_props["s3.endpoint"] = ice.s3_endpoint
    if ice.s3_access_key_id is not None:
        catalog_props["s3.access-key-id"] = ice.s3_access_key_id
    if ice.s3_secret_access_key is not None:
        catalog_props["s3.secret-access-key"] = (
            ice.s3_secret_access_key.get_secret_value()
        )
    catalog_props["s3.region"] = ice.s3_region

    catalog = load_catalog(ice.catalog_name, **catalog_props)
    full_name = f"{ice.table_namespace}.{ice.table_name}"
    return catalog.load_table(full_name), full_name


@lakehouse_app.command("snapshots")
def lakehouse_snapshots(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    sink_id: str | None = typer.Option(None, "--sink-id", help="Iceberg sink ID"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """List snapshots for an Iceberg table."""
    try:
        ice_table, full_name = _load_iceberg_table(
            config_path, platform_config, sink_id
        )
    except Exception as exc:
        console.print(f"[red]Error loading table:[/red] {exc}")
        raise typer.Exit(1) from exc

    from cdc_platform.lakehouse.time_travel import IcebergTimeTravel

    tt = IcebergTimeTravel(ice_table, full_name)
    snapshots = tt.list_snapshots()

    if not snapshots:
        console.print("[yellow]No snapshots found[/yellow]")
        return

    table = Table(title=f"Snapshots — {full_name}")
    table.add_column("Snapshot ID", style="cyan")
    table.add_column("Timestamp (ms)")
    table.add_column("Operation")
    table.add_column("Summary")

    for snap in snapshots:
        table.add_row(
            str(snap.get("snapshot_id", "")),
            str(snap.get("committed_at", "")),
            str(snap.get("operation", "")),
            str(snap.get("summary", "")),
        )

    console.print(table)


@lakehouse_app.command("query")
def lakehouse_query(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    snapshot_id: int = typer.Option(..., "--snapshot-id", help="Snapshot ID to query"),
    limit: int = typer.Option(20, "--limit", help="Max rows to return"),
    sink_id: str | None = typer.Option(None, "--sink-id", help="Iceberg sink ID"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Time-travel query at a specific snapshot."""
    try:
        ice_table, full_name = _load_iceberg_table(
            config_path, platform_config, sink_id
        )
    except Exception as exc:
        console.print(f"[red]Error loading table:[/red] {exc}")
        raise typer.Exit(1) from exc

    from cdc_platform.lakehouse.time_travel import IcebergTimeTravel

    tt = IcebergTimeTravel(ice_table, full_name)
    try:
        arrow_table = tt.scan_at_snapshot(snapshot_id, limit=limit)
    except Exception as exc:
        console.print(f"[red]Query failed:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(
        f"[green]Snapshot {snapshot_id}[/green] — {arrow_table.num_rows} rows"
    )
    console.print(arrow_table.to_pydict())


@lakehouse_app.command("rollback")
def lakehouse_rollback(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    snapshot_id: int = typer.Option(
        ..., "--snapshot-id", help="Snapshot ID to rollback to"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    sink_id: str | None = typer.Option(None, "--sink-id", help="Iceberg sink ID"),
    platform_config: str | None = typer.Option(
        None, "--platform-config", help="Platform YAML"
    ),
) -> None:
    """Rollback an Iceberg table to a previous snapshot."""
    try:
        ice_table, full_name = _load_iceberg_table(
            config_path, platform_config, sink_id
        )
    except Exception as exc:
        console.print(f"[red]Error loading table:[/red] {exc}")
        raise typer.Exit(1) from exc

    if not yes:
        confirm = typer.confirm(f"Rollback '{full_name}' to snapshot {snapshot_id}?")
        if not confirm:
            console.print("[yellow]Cancelled[/yellow]")
            raise typer.Exit(0)

    from cdc_platform.lakehouse.time_travel import IcebergTimeTravel

    tt = IcebergTimeTravel(ice_table, full_name)
    try:
        tt.rollback_to_snapshot(snapshot_id)
        console.print(
            f"[green]Rolled back '{full_name}' to snapshot {snapshot_id}[/green]"
        )
    except Exception as exc:
        console.print(f"[red]Rollback failed:[/red] {exc}")
        raise typer.Exit(1) from exc

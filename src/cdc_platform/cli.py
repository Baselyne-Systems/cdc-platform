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
    platform_config: str | None = typer.Option(None, "--platform-config", help="Platform YAML"),
) -> None:
    """Validate a pipeline configuration file."""
    try:
        pipeline, platform = _load(config_path, platform_config)
        console.print(f"[green]Valid[/green] — pipeline_id={pipeline.pipeline_id}")
        console.print(f"  source: {pipeline.source.source_type} → {pipeline.source.database}")
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
    platform_config: str | None = typer.Option(None, "--platform-config", help="Platform YAML"),
) -> None:
    """Validate config and register the Debezium connector."""
    pipeline, platform = _load(config_path, platform_config)

    async def _deploy() -> None:
        async with DebeziumClient(platform.connector) as client:
            await client.wait_until_ready()
            result = await client.register_connector(pipeline, platform)
            console.print(f"[green]Connector registered:[/green] {result.get('name', 'unknown')}")

    asyncio.run(_deploy())


@app.command()
def health(
    platform_config: str | None = typer.Option(None, "--platform-config", help="Platform YAML"),
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
    platform_config: str | None = typer.Option(None, "--platform-config", help="Platform YAML"),
) -> None:
    """Start a debug console consumer for CDC events."""
    pipeline, platform = _load(config_path, platform_config)

    from cdc_platform.streaming.topics import topics_for_pipeline

    cdc_topics = [
        t for t in topics_for_pipeline(pipeline, platform) if not t.endswith(".dlq")
    ]

    def handler(key: dict[str, Any] | None, value: dict[str, Any] | None, msg: Message) -> None:
        console.print(f"[cyan]{msg.topic()}[/cyan] p={msg.partition()} o={msg.offset()}")
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
    consumer.consume()


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
    platform_config: str | None = typer.Option(None, "--platform-config", help="Platform YAML"),
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

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

from cdc_platform.config.loader import load_pipeline_config
from cdc_platform.config.models import PipelineConfig
from cdc_platform.observability.health import (
    Status,
    check_platform_health,
)
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.streaming.consumer import CDCConsumer

logger = structlog.get_logger()
console = Console()
app = typer.Typer(name="cdc", help="CDC Platform CLI")


def _load(config_path: str) -> PipelineConfig:
    path = Path(config_path)
    if not path.exists():
        console.print(f"[red]Config file not found: {path}[/red]")
        raise typer.Exit(1)
    return load_pipeline_config(path)


@app.command()
def validate(config_path: str = typer.Argument(..., help="Path to pipeline YAML")) -> None:
    """Validate a pipeline configuration file."""
    try:
        pipeline = _load(config_path)
        console.print(f"[green]Valid[/green] — pipeline_id={pipeline.pipeline_id}")
        console.print(f"  source: {pipeline.source.source_type} → {pipeline.source.database}")
        console.print(f"  tables: {pipeline.source.tables}")
        console.print(f"  kafka:  {pipeline.kafka.bootstrap_servers}")
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
def deploy(config_path: str = typer.Argument(..., help="Path to pipeline YAML")) -> None:
    """Validate config and register the Debezium connector."""
    pipeline = _load(config_path)

    async def _deploy() -> None:
        async with DebeziumClient(pipeline.connector) as client:
            await client.wait_until_ready()
            result = await client.register_connector(pipeline)
            console.print(f"[green]Connector registered:[/green] {result.get('name', 'unknown')}")

    asyncio.run(_deploy())


@app.command()
def health(
    bootstrap_servers: str = typer.Option("localhost:9092", help="Kafka bootstrap servers"),
    schema_registry_url: str = typer.Option("http://localhost:8081", help="Schema Registry URL"),
    connect_url: str = typer.Option("http://localhost:8083", help="Kafka Connect URL"),
) -> None:
    """Check health of all platform components."""
    result = check_platform_health(bootstrap_servers, schema_registry_url, connect_url)

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
) -> None:
    """Start a debug console consumer for CDC events."""
    pipeline = _load(config_path)

    from cdc_platform.streaming.topics import topics_for_pipeline

    cdc_topics = [
        t for t in topics_for_pipeline(pipeline) if not t.endswith(".dlq")
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
        kafka_config=pipeline.kafka,
        handler=handler,
    )
    consumer.consume()


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to pipeline YAML"),
) -> None:
    """Run the full CDC pipeline (source → Kafka → sinks)."""
    pipeline = _load(config_path)

    from cdc_platform.pipeline.runner import Pipeline

    console.print(f"[yellow]Starting pipeline:[/yellow] {pipeline.pipeline_id}")
    if pipeline.sinks:
        for s in pipeline.sinks:
            if s.enabled:
                console.print(f"  sink: {s.sink_id} ({s.sink_type})")
    else:
        console.print("  [dim]No sinks configured — events will be consumed only[/dim]")

    runner = Pipeline(pipeline)
    try:
        runner.start()
    except KeyboardInterrupt:
        runner.stop()

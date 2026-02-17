#!/usr/bin/env python3
"""Runnable demo: deploy a Postgres CDC pipeline and consume events.

Prerequisites:
    make up          # starts Docker Compose services
    uv run python examples/postgres_cdc_demo.py
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

from rich.console import Console
from confluent_kafka import Message

from cdc_platform.config.templates import build_pipeline_config
from cdc_platform.observability.health import check_platform_health, Status
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.sources.debezium.config import connector_name
from cdc_platform.streaming.consumer import CDCConsumer
from cdc_platform.streaming.topics import topics_for_pipeline

console = Console()


def main() -> None:
    # 1. Build config from template + minimal overrides
    pipeline = build_pipeline_config(
        {
            "pipeline_id": "demo",
            "source": {
                "database": "cdc_demo",
                "password": "cdc_password",
                "tables": ["public.customers", "public.orders"],
            },
        },
    )
    console.print("[bold]Pipeline config built[/bold]", pipeline.pipeline_id)

    # 2. Health check
    health = check_platform_health()
    if not health.healthy:
        console.print("[red]Platform not healthy:[/red]", health.summary)
        console.print("Run 'make up' first.")
        sys.exit(1)
    console.print("[green]All components healthy[/green]")

    # 3. Register connector
    async def deploy() -> None:
        async with DebeziumClient(pipeline.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(pipeline)
            console.print(
                f"[green]Connector registered:[/green] {connector_name(pipeline)}"
            )

    asyncio.run(deploy())

    # 4. Consume CDC events
    cdc_topics = [
        t for t in topics_for_pipeline(pipeline) if not t.endswith(".dlq")
    ]
    console.print(f"[yellow]Consuming from:[/yellow] {cdc_topics}")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    def handler(
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        msg: Message,
    ) -> None:
        op = value.get("op", "?") if value else "?"
        console.print(
            f"[cyan]{msg.topic()}[/cyan]  op={op}  "
            f"partition={msg.partition()} offset={msg.offset()}"
        )
        if value and value.get("after"):
            console.print(f"  after: {value['after']}")
        console.print()

    consumer = CDCConsumer(
        topics=cdc_topics,
        kafka_config=pipeline.kafka,
        handler=handler,
    )
    consumer.consume()


if __name__ == "__main__":
    main()

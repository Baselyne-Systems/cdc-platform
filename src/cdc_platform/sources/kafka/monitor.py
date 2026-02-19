"""Kafka SourceMonitor — composes SchemaMonitor + LagMonitor."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cdc_platform.config.models import KafkaConfig
from cdc_platform.observability.metrics import LagMonitor
from cdc_platform.streaming.schema_monitor import SchemaMonitor


class KafkaSourceMonitor:
    """Composes SchemaMonitor and LagMonitor behind the SourceMonitor protocol."""

    def __init__(
        self,
        kafka_config: KafkaConfig,
        topics: list[str],
        schema_monitor_interval: float = 30.0,
        lag_monitor_interval: float = 15.0,
        stop_on_incompatible: bool = False,
        on_incompatible: Callable[[], None] | None = None,
    ) -> None:
        self._schema_monitor = SchemaMonitor(
            registry_url=kafka_config.schema_registry_url,
            topics=topics,
            interval=schema_monitor_interval,
            stop_on_incompatible=stop_on_incompatible,
            on_incompatible=on_incompatible,
        )
        self._lag_monitor = LagMonitor(
            bootstrap_servers=kafka_config.bootstrap_servers,
            group_id=kafka_config.group_id,
            topics=topics,
            interval=lag_monitor_interval,
        )

    async def start(self) -> None:
        await self._schema_monitor.start()
        await self._lag_monitor.start()

    async def stop(self) -> None:
        await self._schema_monitor.stop()
        await self._lag_monitor.stop()

    async def get_lag(self) -> list[dict[str, Any]]:
        return [
            {
                "topic": p.topic,
                "partition": p.partition,
                "lag": p.lag,
                "offset": p.current_offset,
            }
            for p in self._lag_monitor.latest_lag
        ]

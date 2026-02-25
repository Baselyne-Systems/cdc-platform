"""Basic consumer lag metrics."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class PartitionLag:
    topic: str
    partition: int
    current_offset: int
    high_watermark: int
    lag: int


def get_consumer_lag(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
) -> list[PartitionLag]:
    """Query consumer group lag without joining the main consumer group.

    Uses AdminClient to fetch committed offsets for the real group.id,
    and a separate Consumer with an isolated group.id only for
    get_watermark_offsets() (metadata call, never joins the main group).
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Use an isolated consumer only for watermark offset queries
    from confluent_kafka import Consumer

    lag_consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"__cdc_lag_monitor_{group_id}",
            "enable.auto.commit": False,
        }
    )

    results: list[PartitionLag] = []
    try:
        for topic in topics:
            meta = lag_consumer.list_topics(topic=topic, timeout=5)
            partitions = meta.topics.get(topic)
            if not partitions:
                continue

            tps = [TopicPartition(topic, p) for p in partitions.partitions]

            # Query committed offsets for the real group via AdminClient
            futures = admin.list_consumer_group_offsets(
                [{"group": group_id, "partitions": tps}]
            )
            # list_consumer_group_offsets returns {group_id: future}
            group_future = futures[group_id]
            committed_tps = group_future.result()

            for tp_result in committed_tps:
                _, hi = lag_consumer.get_watermark_offsets(tp_result, timeout=5)
                current = tp_result.offset if tp_result.offset >= 0 else 0
                results.append(
                    PartitionLag(
                        topic=tp_result.topic,
                        partition=tp_result.partition,
                        current_offset=current,
                        high_watermark=hi,
                        lag=hi - current,
                    )
                )
    finally:
        lag_consumer.close()

    return results


class LagMonitor:
    """Periodic consumer lag reporter."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        interval: float = 15.0,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._interval = interval
        self._task: asyncio.Task | None = None  # type: ignore[type-arg]
        self._latest_lag: list[PartitionLag] = []

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _poll_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(self._interval)
            try:
                self._latest_lag = await loop.run_in_executor(
                    None,
                    get_consumer_lag,
                    self._bootstrap_servers,
                    self._group_id,
                    self._topics,
                )
                total_lag = sum(p.lag for p in self._latest_lag)
                logger.info(
                    "consumer.lag",
                    total_lag=total_lag,
                    partitions=[
                        {
                            "topic": p.topic,
                            "partition": p.partition,
                            "lag": p.lag,
                            "offset": p.current_offset,
                        }
                        for p in self._latest_lag
                    ],
                )
            except Exception as exc:
                logger.warning("consumer.lag_check_failed", error=str(exc))

    @property
    def latest_lag(self) -> list[PartitionLag]:
        return list(self._latest_lag)

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
    """Query consumer group lag for the given topics."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )

    results: list[PartitionLag] = []
    try:
        for topic in topics:
            meta = consumer.list_topics(topic=topic, timeout=5)
            partitions = meta.topics.get(topic)
            if not partitions:
                continue

            from confluent_kafka import TopicPartition

            tps = [TopicPartition(topic, p) for p in partitions.partitions]
            committed = consumer.committed(tps, timeout=5)

            for tp in committed:
                _, hi = consumer.get_watermark_offsets(tp, timeout=5)
                current = tp.offset if tp.offset >= 0 else 0
                results.append(
                    PartitionLag(
                        topic=tp.topic,
                        partition=tp.partition,
                        current_offset=current,
                        high_watermark=hi,
                        lag=hi - current,
                    )
                )
    finally:
        consumer.close()

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

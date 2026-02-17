"""Basic consumer lag metrics."""

from __future__ import annotations

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

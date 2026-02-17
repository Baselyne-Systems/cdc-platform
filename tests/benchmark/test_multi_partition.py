import uuid
from typing import Any

import pytest
import structlog

from .helpers import (
    AvroProducer,
    CountingSink,
    InstrumentedSink,
    consume_with_sink,
    create_benchmark_topic,
)

logger = structlog.get_logger()
PARTITION_COUNTS = [1, 4, 8]


@pytest.mark.benchmark
class TestMultiPartition:
    """Measure throughput scaling with multiple partitions."""

    @pytest.mark.parametrize("num_partitions", PARTITION_COUNTS, ids=["1P", "4P", "8P"])
    async def test_partition_scaling(
        self,
        num_partitions: int,
        kafka_admin: Any,
        platform_config: Any,
        benchmark_report: Any,
    ) -> None:
        """Measure throughput for N partitions."""
        message_count = 5_000
        topic_name = f"bench.partitions.{num_partitions}.{uuid.uuid4().hex[:8]}"

        logger.info(
            "benchmark.partitions.start",
            num_partitions=num_partitions,
            message_count=message_count,
        )

        # 1. Create topic
        create_benchmark_topic(kafka_admin, topic_name, num_partitions=num_partitions)

        # 2. Pre-produce messages
        # Distribute messages somewhat evenly?
        # AvroProducer uses default partitioner (consistent random).
        producer = AvroProducer(
            topic_name,
            platform_config.kafka.bootstrap_servers,
            platform_config.kafka.schema_registry_url,
        )
        elapsed_prod = producer.produce_batch(message_count)

        logger.info(
            "benchmark.partitions.produced", count=message_count, elapsed=elapsed_prod
        )

        # 3. Consume
        real_sink = CountingSink(expected=message_count)
        sink = InstrumentedSink(
            real_sink, expected_count=message_count, track_e2e_latency=False
        )

        result = await consume_with_sink(
            topic=topic_name,
            sink=sink,
            expected_count=message_count,
            kafka_config=platform_config.kafka,
            timeout=30.0,
        )

        # 4. Report
        result.name = f"partitions-{num_partitions}"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.partitions.complete",
            num_partitions=num_partitions,
            throughput=result.throughput_msg_per_sec,
            duration=result.duration_seconds,
        )

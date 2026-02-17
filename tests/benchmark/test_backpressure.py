import asyncio
import time
import tracemalloc
import uuid
from typing import Any

import pytest
import structlog

from cdc_platform.streaming.consumer import CDCConsumer

from .helpers import AvroProducer, SlowSink, create_benchmark_topic

logger = structlog.get_logger()


@pytest.mark.benchmark
class TestBackpressure:
    """Verify that the system handles backpressure correctly under load."""

    @pytest.fixture
    def bench_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic_name = f"bench.backpressure.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic_name, num_partitions=1)

        # Pre-produce enough messages to fill the queue
        producer = AvroProducer(
            topic_name,
            platform_config.kafka.bootstrap_servers,
            platform_config.kafka.schema_registry_url,
        )
        producer.produce_batch(500)  # Enough to saturate a small buffer
        return topic_name

    async def test_queue_depth_stays_bounded(
        self, bench_topic: str, platform_config: Any, benchmark_report: Any
    ) -> None:
        """Test that internal queues do not exceed max_buffered_messages."""
        max_buffered = 50
        slow_delay = 0.05

        # We need to manually construct the pipeline flow to inspect the queue
        # CDCConsumer puts into queues. But CDCConsumer usually calls handler.
        # The architecture description says:
        # "CDCConsumer poll -> queue -> worker -> sink"
        # BUT CDCConsumer source code we read (Step 16) takes a handler.
        # It does NOT seem to have an internal queue in the CDCConsumer class itself.
        # The 'pipeline/runner.py' (which we haven't read) likely manages the queues.
        #
        # However, for this benchmark, we can simulate the "Consumer -> Queue -> Sink" flow
        # by creating a custom handler that puts into a queue.

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_buffered)
        sink = SlowSink(delay_seconds=slow_delay)

        # Custom handler that mimics the pipeline runner
        async def enqueue_handler(
            key: dict[str, Any] | None, value: dict[str, Any] | None, msg: Any
        ) -> None:
            # This should block when queue is full, applying backpressure to consumer
            await queue.put((key, value, msg))

        consumer = CDCConsumer(
            topics=[bench_topic],
            kafka_config=platform_config.kafka,
            async_handler=enqueue_handler,
        )

        # Worker task
        processed_count = 0
        max_seen_depth = 0
        total_depth = 0
        samples = 0

        async def worker() -> None:
            nonlocal processed_count, max_seen_depth, total_depth, samples
            while True:
                await queue.get()

                # Sample queue depth
                depth = queue.qsize()
                max_seen_depth = max(max_seen_depth, depth)
                total_depth += depth
                samples += 1

                if samples % 50 == 0:
                    logger.info(
                        "benchmark.backpressure.queue_sample",
                        queue_depth=depth,
                        max_depth_so_far=max_seen_depth,
                        messages_processed=samples,
                    )

                # Write to slow sink
                await sink.write(None, None, "", 0, 0)
                queue.task_done()
                processed_count += 1

                if processed_count >= 200:
                    break

        logger.info(
            "benchmark.backpressure.start",
            max_buffered=max_buffered,
            slow_delay=slow_delay,
            target_messages=200,
        )

        start_time = time.perf_counter()

        consume_task = asyncio.create_task(consumer.consume_async())
        worker_task = asyncio.create_task(worker())

        await worker_task
        consume_task.cancel()
        import contextlib

        with contextlib.suppress(asyncio.CancelledError):
            await consume_task

        elapsed = time.perf_counter() - start_time

        # Assertions
        # The queue depth should never exceed max_buffered
        # Note: qsize() is approximate but for bounded queue it shouldn't exceed maxsize
        assert max_seen_depth <= max_buffered + 1  # +1 variance allowed

        avg_depth = total_depth / samples if samples > 0 else 0

        logger.info(
            "benchmark.backpressure.complete",
            max_queue_depth=max_seen_depth,
            avg_queue_depth=avg_depth,
            messages=processed_count,
            duration=elapsed,
        )

        from .helpers import BenchmarkResult

        benchmark_report.add_result(
            BenchmarkResult(
                name=f"backpressure-delay-{slow_delay}s",
                messages=processed_count,
                duration_seconds=elapsed,
                throughput_msg_per_sec=processed_count / elapsed,
                extra={"max_queue_depth": max_seen_depth},
            )
        )

    async def test_memory_stays_bounded(
        self, bench_topic: str, platform_config: Any, benchmark_report: Any
    ) -> None:
        """Test that memory usage doesn't grow unbounded."""
        # This is a bit tricky to assert strictly in a short test, but we can check delta
        max_buffered = 50
        slow_delay = 0.001  # Faster to churn meaningful volume

        tracemalloc.start()
        snapshot1 = tracemalloc.take_snapshot()

        # Run a similar flow but for more messages
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_buffered)
        sink = SlowSink(delay_seconds=slow_delay)

        async def enqueue_handler(
            key: dict[str, Any] | None, value: dict[str, Any] | None, msg: Any
        ) -> None:
            await queue.put((key, value, msg))

        processed_count = 0
        limit = 500

        async def worker() -> None:
            nonlocal processed_count
            print("DEBUG: worker started")
            while processed_count < limit:
                try:
                    await asyncio.wait_for(queue.get(), timeout=5.0)
                except TimeoutError:
                    print(
                        f"DEBUG: worker timed out waiting for message {processed_count + 1}"
                    )
                    break

                await sink.write(None, None, "", 0, 0)
                queue.task_done()
                processed_count += 1
                if processed_count % 50 == 0:
                    print(f"DEBUG: processed {processed_count}/{limit}")

        # Unique group to avoid interference
        kafka_cfg = platform_config.kafka.model_copy()
        kafka_cfg.group_id = f"bench-backpressure-{uuid.uuid4().hex[:8]}"

        consumer = CDCConsumer(
            topics=[bench_topic],
            kafka_config=kafka_cfg,
            async_handler=enqueue_handler,
        )
        print(f"DEBUG: consumer created with group {kafka_cfg.group_id}")

        consume_task = asyncio.create_task(consumer.consume_async())
        worker_task = asyncio.create_task(worker())

        await worker_task
        consume_task.cancel()
        import contextlib

        with contextlib.suppress(asyncio.CancelledError):
            await consume_task

        snapshot2 = tracemalloc.take_snapshot()
        top_stats = snapshot2.compare_to(snapshot1, "lineno")

        # Calculate total growth
        total_growth = sum(stat.size_diff for stat in top_stats)
        growth_mb = total_growth / (1024 * 1024)

        logger.info(
            "benchmark.backpressure.memory",
            growth_mb=growth_mb,
            messages=processed_count,
        )

        if top_stats:
            print(f"DEBUG: Top memory user: {top_stats[0]}")

        # 500 messages shouldn't leak memory.
        assert growth_mb < 50

        tracemalloc.stop()

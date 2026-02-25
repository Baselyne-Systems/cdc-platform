"""PubSubProvisioner — creates Pub/Sub topics, subscriptions, and DLQ."""

from __future__ import annotations

from typing import Any

import structlog

from cdc_platform.config.models import PipelineConfig, PlatformConfig
from cdc_platform.sources.pubsub.naming import (
    pubsub_dlq_topic_name,
    pubsub_subscription_name,
    pubsub_topic_name,
)
from cdc_platform.streaming.topics import topics_for_pipeline

logger = structlog.get_logger()


class PubSubProvisioner:
    """Creates Pub/Sub topics + subscriptions + DLQ topics.

    Also creates the PG replication slot/publication via SlotManager
    since Pub/Sub mode uses the direct WAL reader instead of Debezium.
    """

    def __init__(self, platform: PlatformConfig) -> None:
        self._platform = platform

    async def provision(self, pipeline: PipelineConfig) -> dict[str, Any]:
        assert self._platform.pubsub is not None
        assert self._platform.wal_reader is not None

        from google.api_core.exceptions import AlreadyExists
        from google.cloud import pubsub_v1

        config = self._platform.pubsub
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        all_cdc_topics = topics_for_pipeline(pipeline, self._platform)
        cdc_topics = [t for t in all_cdc_topics if not t.endswith(".dlq")]

        created_topics: list[str] = []
        created_subscriptions: list[str] = []

        try:
            for cdc_topic in cdc_topics:
                # Create topic
                full_topic = pubsub_topic_name(config.project_id, cdc_topic)
                try:
                    if config.ordering_enabled:
                        publisher.create_topic(
                            request={
                                "name": full_topic,
                            }
                        )
                    else:
                        publisher.create_topic(request={"name": full_topic})
                    created_topics.append(full_topic)
                    logger.info("pubsub.topic_created", topic=full_topic)
                except AlreadyExists:
                    logger.info("pubsub.topic_exists", topic=full_topic)

                # Create DLQ topic (outside subscription try/except so errors propagate)
                dlq_topic: str | None = None
                if self._platform.dlq.enabled:
                    dlq_topic = pubsub_dlq_topic_name(
                        config.project_id, cdc_topic, self._platform.dlq.topic_suffix
                    )
                    try:
                        publisher.create_topic(request={"name": dlq_topic})
                        created_topics.append(dlq_topic)
                        logger.info("pubsub.dlq_topic_created", topic=dlq_topic)
                    except AlreadyExists:
                        pass

                # Create subscription
                sub_name = pubsub_subscription_name(
                    config.project_id, cdc_topic, config.group_id
                )
                try:
                    request: dict[str, Any] = {
                        "name": sub_name,
                        "topic": full_topic,
                        "ack_deadline_seconds": config.ack_deadline_seconds,
                        "enable_message_ordering": config.ordering_enabled,
                    }

                    if dlq_topic is not None:
                        request["dead_letter_policy"] = {
                            "dead_letter_topic": dlq_topic,
                            "max_delivery_attempts": config.max_delivery_attempts,
                        }

                    subscriber.create_subscription(request=request)
                    created_subscriptions.append(sub_name)
                    logger.info("pubsub.subscription_created", subscription=sub_name)
                except AlreadyExists:
                    logger.info("pubsub.subscription_exists", subscription=sub_name)

            # Set up PG replication slot + publication
            from cdc_platform.sources.wal.slot_manager import SlotManager

            source = pipeline.source
            dsn = (
                f"host={source.host} port={source.port} "
                f"dbname={source.database} user={source.username} "
                f"password={source.password.get_secret_value()}"
            )
            slot_mgr = SlotManager(
                dsn=dsn,
                slot_name=self._platform.wal_reader.slot_name,
                publication_name=self._platform.wal_reader.publication_name,
            )
            await slot_mgr.ensure_publication(source.tables)
            await slot_mgr.ensure_slot()
        except Exception:
            logger.error(
                "pubsub.provision_failed, rolling back",
                pipeline_id=pipeline.pipeline_id,
            )
            self._rollback(subscriber, publisher, created_subscriptions, created_topics)
            raise

        return {
            "topics": created_topics,
            "subscriptions": created_subscriptions,
        }

    def _rollback(
        self,
        subscriber: Any,
        publisher: Any,
        subscriptions: list[str],
        topics: list[str],
    ) -> None:
        """Best-effort cleanup of resources created during a failed provision."""
        for sub in subscriptions:
            try:
                subscriber.delete_subscription(request={"subscription": sub})
                logger.info("pubsub.rollback_subscription_deleted", subscription=sub)
            except Exception as exc:
                logger.warning(
                    "pubsub.rollback_subscription_delete_failed",
                    subscription=sub,
                    error=str(exc),
                )
        for topic in topics:
            try:
                publisher.delete_topic(request={"topic": topic})
                logger.info("pubsub.rollback_topic_deleted", topic=topic)
            except Exception as exc:
                logger.warning(
                    "pubsub.rollback_topic_delete_failed",
                    topic=topic,
                    error=str(exc),
                )

    async def teardown(self, pipeline: PipelineConfig) -> None:
        assert self._platform.pubsub is not None

        from google.cloud import pubsub_v1

        config = self._platform.pubsub
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        all_cdc_topics = topics_for_pipeline(pipeline, self._platform)
        cdc_topics = [t for t in all_cdc_topics if not t.endswith(".dlq")]

        for cdc_topic in cdc_topics:
            sub_name = pubsub_subscription_name(
                config.project_id, cdc_topic, config.group_id
            )
            try:
                subscriber.delete_subscription(request={"subscription": sub_name})
                logger.info("pubsub.subscription_deleted", subscription=sub_name)
            except Exception as exc:
                logger.warning(
                    "pubsub.subscription_delete_failed",
                    subscription=sub_name,
                    error=str(exc),
                )

            full_topic = pubsub_topic_name(config.project_id, cdc_topic)
            try:
                publisher.delete_topic(request={"topic": full_topic})
                logger.info("pubsub.topic_deleted", topic=full_topic)
            except Exception as exc:
                logger.warning(
                    "pubsub.topic_delete_failed",
                    topic=full_topic,
                    error=str(exc),
                )

            # Delete DLQ topic
            if self._platform.dlq.enabled:
                dlq_topic = pubsub_dlq_topic_name(
                    config.project_id, cdc_topic, self._platform.dlq.topic_suffix
                )
                try:
                    publisher.delete_topic(request={"topic": dlq_topic})
                    logger.info("pubsub.dlq_topic_deleted", topic=dlq_topic)
                except Exception as exc:
                    logger.warning(
                        "pubsub.dlq_topic_delete_failed",
                        topic=dlq_topic,
                        error=str(exc),
                    )

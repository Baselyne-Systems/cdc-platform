"""Pub/Sub topic and subscription naming conventions."""

from __future__ import annotations


def pubsub_topic_name(project_id: str, cdc_topic: str) -> str:
    """Build a fully-qualified Pub/Sub topic name.

    Converts dots to hyphens since Pub/Sub topic names cannot contain dots.
    """
    safe_name = cdc_topic.replace(".", "-")
    return f"projects/{project_id}/topics/{safe_name}"


def pubsub_subscription_name(project_id: str, cdc_topic: str, group_id: str) -> str:
    """Build a fully-qualified Pub/Sub subscription name."""
    safe_name = cdc_topic.replace(".", "-")
    return f"projects/{project_id}/subscriptions/{safe_name}-{group_id}"


def pubsub_dlq_topic_name(project_id: str, cdc_topic: str, suffix: str = "dlq") -> str:
    """Build a fully-qualified DLQ topic name."""
    safe_name = cdc_topic.replace(".", "-")
    return f"projects/{project_id}/topics/{safe_name}-{suffix}"


def cdc_topic_from_pubsub(pubsub_topic: str) -> str:
    """Extract the logical CDC topic name from a Pub/Sub topic path."""
    # projects/{project}/topics/{name}
    name = pubsub_topic.rsplit("/", 1)[-1]
    return name.replace("-", ".")

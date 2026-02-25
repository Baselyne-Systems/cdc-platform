"""Kinesis stream naming conventions."""

from __future__ import annotations


def kinesis_stream_name(cdc_topic: str) -> str:
    """Convert a CDC topic name to a Kinesis stream name.

    Replaces dots with hyphens since Kinesis stream names have restricted chars.
    """
    return cdc_topic.replace(".", "-")


def kinesis_dlq_stream_name(cdc_topic: str, suffix: str = "dlq") -> str:
    """Build a DLQ stream name."""
    return f"{kinesis_stream_name(cdc_topic)}-{suffix}"


def cdc_topic_from_stream(stream_name: str) -> str:
    """Convert a Kinesis stream name back to a CDC topic name."""
    return stream_name.replace("-", ".")

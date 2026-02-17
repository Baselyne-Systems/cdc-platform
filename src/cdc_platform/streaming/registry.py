"""Thin wrapper around Confluent Schema Registry client."""

from __future__ import annotations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer


def create_registry_client(url: str) -> SchemaRegistryClient:
    """Create a Schema Registry client."""
    return SchemaRegistryClient({"url": url})


def create_avro_serializer(
    registry: SchemaRegistryClient,
    schema_str: str,
) -> AvroSerializer:
    """Create an Avro serializer bound to the registry."""
    return AvroSerializer(registry, schema_str)


def create_avro_deserializer(
    registry: SchemaRegistryClient,
) -> AvroDeserializer:
    """Create an Avro deserializer that looks up schemas in the registry."""
    return AvroDeserializer(registry)

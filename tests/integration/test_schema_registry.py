"""Verify that Debezium auto-registers Avro schemas in Schema Registry."""

from __future__ import annotations

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient


@pytest.mark.integration
class TestSchemaRegistry:
    def test_subjects_registered_after_snapshot(self, docker_services, _register_connector):
        """After connector starts, subjects should appear in the registry."""
        import time

        client = SchemaRegistryClient({"url": "http://localhost:8081"})

        # Give Debezium time to register schemas
        deadline = time.time() + 60
        subjects: list[str] = []
        while time.time() < deadline:
            subjects = client.get_subjects()
            if any("customers" in s for s in subjects):
                break
            time.sleep(2)

        customer_subjects = [s for s in subjects if "customers" in s]
        assert len(customer_subjects) >= 1, f"Expected customers subjects, got: {subjects}"

    def test_schema_contains_expected_fields(self, docker_services, _register_connector):
        """The registered value schema should contain expected column names."""
        import json
        import time

        client = SchemaRegistryClient({"url": "http://localhost:8081"})

        # Find the value subject
        deadline = time.time() + 30
        value_subject = None
        while time.time() < deadline:
            subjects = client.get_subjects()
            for s in subjects:
                if "customers" in s and "value" in s.lower():
                    value_subject = s
                    break
            if value_subject:
                break
            time.sleep(2)

        assert value_subject is not None, "No customers value subject found"

        schema = client.get_latest_version(value_subject)
        schema_dict = json.loads(schema.schema.schema_str)
        # Debezium wraps in an Envelope with 'before', 'after', 'op', etc.
        field_names = [f["name"] for f in schema_dict.get("fields", [])]
        assert "before" in field_names or "after" in field_names

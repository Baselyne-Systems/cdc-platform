"""Unit tests for the Debezium client using respx to mock httpx."""

import httpx
import pytest
import respx

from cdc_platform.config.models import ConnectorConfig, PipelineConfig, SourceConfig
from cdc_platform.sources.debezium.client import ConnectError, DebeziumClient
from cdc_platform.sources.debezium.config import (
    build_postgres_connector_config,
    connector_name,
)

CONNECT_URL = "http://connect:8083"


@pytest.fixture
def pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test-pg",
        source=SourceConfig(
            database="testdb",
            password="secret",
            tables=["public.customers"],
        ),
    )


@pytest.fixture
def config() -> ConnectorConfig:
    return ConnectorConfig(connect_url=CONNECT_URL)


class TestConnectorConfig:
    def test_connector_name(self, pipeline: PipelineConfig):
        assert connector_name(pipeline) == "cdc-test-pg"

    def test_build_config_includes_avro(self, pipeline: PipelineConfig):
        cfg = build_postgres_connector_config(pipeline)
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert cfg["plugin.name"] == "pgoutput"
        assert cfg["database.dbname"] == "testdb"
        assert cfg["table.include.list"] == "public.customers"


class TestDebeziumClient:
    @pytest.mark.asyncio
    @respx.mock
    async def test_register_connector_success(
        self, pipeline: PipelineConfig, config: ConnectorConfig
    ):
        name = connector_name(pipeline)
        route = respx.put(f"{CONNECT_URL}/connectors/{name}/config").mock(
            return_value=httpx.Response(201, json={"name": name, "config": {}})
        )
        async with DebeziumClient(config) as client:
            result = await client.register_connector(pipeline)
        assert route.called
        assert result["name"] == name

    @pytest.mark.asyncio
    @respx.mock
    async def test_register_connector_failure_raises(
        self, pipeline: PipelineConfig, config: ConnectorConfig
    ):
        name = connector_name(pipeline)
        respx.put(f"{CONNECT_URL}/connectors/{name}/config").mock(
            return_value=httpx.Response(400, text="Bad config")
        )
        async with DebeziumClient(config) as client:
            with pytest.raises(ConnectError, match="400"):
                await client.register_connector(pipeline)

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_connector_status(self, config: ConnectorConfig):
        respx.get(f"{CONNECT_URL}/connectors/my-conn/status").mock(
            return_value=httpx.Response(
                200, json={"name": "my-conn", "connector": {"state": "RUNNING"}}
            )
        )
        async with DebeziumClient(config) as client:
            status = await client.get_connector_status("my-conn")
        assert status["connector"]["state"] == "RUNNING"

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_connectors(self, config: ConnectorConfig):
        respx.get(f"{CONNECT_URL}/connectors").mock(
            return_value=httpx.Response(200, json=["conn-a", "conn-b"])
        )
        async with DebeziumClient(config) as client:
            result = await client.list_connectors()
        assert result == ["conn-a", "conn-b"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_delete_connector(self, config: ConnectorConfig):
        route = respx.delete(f"{CONNECT_URL}/connectors/my-conn").mock(
            return_value=httpx.Response(204)
        )
        async with DebeziumClient(config) as client:
            await client.delete_connector("my-conn")
        assert route.called

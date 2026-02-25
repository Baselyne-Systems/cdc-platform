"""Unit tests for the Debezium client using respx to mock httpx."""

import httpx
import pytest
import respx

from cdc_platform.config.models import (
    ConnectorConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
    SourceType,
)
from cdc_platform.sources.debezium.client import ConnectError, DebeziumClient
from cdc_platform.sources.debezium.config import (
    build_connector_config,
    build_mysql_connector_config,
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
def platform() -> PlatformConfig:
    return PlatformConfig()


@pytest.fixture
def config() -> ConnectorConfig:
    return ConnectorConfig(connect_url=CONNECT_URL)


class TestConnectorConfig:
    def test_connector_name(self, pipeline: PipelineConfig):
        assert connector_name(pipeline) == "cdc-test-pg"

    def test_build_config_includes_avro_converter(
        self, pipeline: PipelineConfig, platform: PlatformConfig
    ):
        cfg = build_postgres_connector_config(pipeline, platform)
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert cfg["plugin.name"] == "pgoutput"
        assert cfg["database.dbname"] == "testdb"
        assert cfg["table.include.list"] == "public.customers"


class TestMySQLConnectorConfig:
    def test_build_mysql_config(self):
        pipeline = PipelineConfig(
            pipeline_id="test-mysql",
            source=SourceConfig(
                source_type=SourceType.MYSQL,
                host="mysql-host",
                port=3306,
                database="testdb",
                password="secret",
                tables=["mydb.users", "mydb.orders"],
            ),
        )
        platform = PlatformConfig()
        cfg = build_mysql_connector_config(pipeline, platform)
        assert cfg["connector.class"] == "io.debezium.connector.mysql.MySqlConnector"
        assert cfg["database.hostname"] == "mysql-host"
        assert cfg["database.port"] == "3306"
        assert cfg["database.include.list"] == "testdb"
        assert cfg["table.include.list"] == "mydb.users,mydb.orders"
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert "schema.history.internal.kafka.bootstrap.servers" in cfg
        assert "schema.history.internal.kafka.topic" in cfg

    def test_build_connector_config_dispatches_mysql(self):
        pipeline = PipelineConfig(
            pipeline_id="test-mysql",
            source=SourceConfig(
                source_type=SourceType.MYSQL,
                database="testdb",
                tables=["mydb.t"],
            ),
        )
        platform = PlatformConfig()
        cfg = build_connector_config(pipeline, platform)
        assert "MySqlConnector" in cfg["connector.class"]

    def test_build_connector_config_dispatches_postgres(
        self, pipeline: PipelineConfig, platform: PlatformConfig
    ):
        cfg = build_connector_config(pipeline, platform)
        assert "PostgresConnector" in cfg["connector.class"]


class TestDebeziumClient:
    @pytest.mark.asyncio
    @respx.mock
    async def test_register_connector_success(
        self,
        pipeline: PipelineConfig,
        platform: PlatformConfig,
        config: ConnectorConfig,
    ):
        name = connector_name(pipeline)
        route = respx.put(f"{CONNECT_URL}/connectors/{name}/config").mock(
            return_value=httpx.Response(201, json={"name": name, "config": {}})
        )
        async with DebeziumClient(config) as client:
            result = await client.register_connector(pipeline, platform)
        assert route.called
        assert result["name"] == name

    @pytest.mark.asyncio
    @respx.mock
    async def test_register_connector_failure_raises(
        self,
        pipeline: PipelineConfig,
        platform: PlatformConfig,
        config: ConnectorConfig,
    ):
        name = connector_name(pipeline)
        respx.put(f"{CONNECT_URL}/connectors/{name}/config").mock(
            return_value=httpx.Response(400, text="Bad config")
        )
        async with DebeziumClient(config) as client:
            with pytest.raises(ConnectError, match="400"):
                await client.register_connector(pipeline, platform)

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

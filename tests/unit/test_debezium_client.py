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
    build_mongodb_connector_config,
    build_mysql_connector_config,
    build_postgres_connector_config,
    build_sqlserver_connector_config,
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

    def test_mysql_server_id_propagated(self):
        pipeline = PipelineConfig(
            pipeline_id="test-mysql",
            source=SourceConfig(
                source_type=SourceType.MYSQL,
                database="testdb",
                tables=["mydb.t"],
                mysql_server_id=99,
            ),
        )
        cfg = build_mysql_connector_config(pipeline, PlatformConfig())
        assert cfg["database.server.id"] == "99"


class TestMongoDBConnectorConfig:
    @pytest.fixture
    def mongo_pipeline(self) -> PipelineConfig:
        return PipelineConfig(
            pipeline_id="test-mongo",
            source=SourceConfig(
                source_type=SourceType.MONGODB,
                host="mongo-host",
                port=27017,
                database="mydb",
                username="cdc_user",
                password="secret",
                tables=["mydb.orders", "mydb.customers"],
                replica_set_name="rs0",
            ),
        )

    def test_connector_class(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert (
            cfg["connector.class"] == "io.debezium.connector.mongodb.MongoDbConnector"
        )

    def test_connection_string_contains_host_and_port(
        self, mongo_pipeline: PipelineConfig
    ):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert "mongo-host:27017" in cfg["mongodb.connection.string"]

    def test_connection_string_contains_replica_set(
        self, mongo_pipeline: PipelineConfig
    ):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert "replicaSet=rs0" in cfg["mongodb.connection.string"]

    def test_connection_string_no_replica_set_when_absent(self):
        pipeline = PipelineConfig(
            pipeline_id="test-mongo",
            source=SourceConfig(
                source_type=SourceType.MONGODB,
                host="mongo-host",
                port=27017,
                database="mydb",
                tables=["mydb.events"],
            ),
        )
        cfg = build_mongodb_connector_config(pipeline, PlatformConfig())
        assert "replicaSet" not in cfg["mongodb.connection.string"]

    def test_collection_include_list(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert cfg["collection.include.list"] == "mydb.orders,mydb.customers"

    def test_no_table_include_list_key(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert "table.include.list" not in cfg

    def test_capture_mode_full_document(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert cfg["capture.mode"] == "change_streams_update_full"

    def test_avro_converters(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert cfg["value.converter"] == "io.confluent.connect.avro.AvroConverter"

    def test_topic_prefix(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert cfg["topic.prefix"] == "cdc"

    def test_snapshot_mode(self, mongo_pipeline: PipelineConfig):
        cfg = build_mongodb_connector_config(mongo_pipeline, PlatformConfig())
        assert cfg["snapshot.mode"] == "initial"

    def test_dispatch_to_mongodb(self, mongo_pipeline: PipelineConfig):
        cfg = build_connector_config(mongo_pipeline, PlatformConfig())
        assert "MongoDb" in cfg["connector.class"]

    def test_custom_auth_source(self):
        pipeline = PipelineConfig(
            pipeline_id="test-mongo",
            source=SourceConfig(
                source_type=SourceType.MONGODB,
                host="mongo-host",
                port=27017,
                database="mydb",
                tables=["mydb.events"],
                auth_source="mydb",
            ),
        )
        cfg = build_mongodb_connector_config(pipeline, PlatformConfig())
        assert "authSource=mydb" in cfg["mongodb.connection.string"]


class TestSQLServerConnectorConfig:
    @pytest.fixture
    def sqlserver_pipeline(self) -> PipelineConfig:
        return PipelineConfig(
            pipeline_id="test-sqlserver",
            source=SourceConfig(
                source_type=SourceType.SQLSERVER,
                host="sqlserver-host",
                port=1433,
                database="testdb",
                username="sa",
                password="secret",
                tables=["dbo.customers", "sales.orders"],
            ),
        )

    def test_connector_class(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert (
            cfg["connector.class"]
            == "io.debezium.connector.sqlserver.SqlServerConnector"
        )

    def test_host_and_port(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["database.hostname"] == "sqlserver-host"
        assert cfg["database.port"] == "1433"

    def test_database_names(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["database.names"] == "testdb"

    def test_table_include_list(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["table.include.list"] == "dbo.customers,sales.orders"

    def test_schema_history_present(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert "schema.history.internal.kafka.bootstrap.servers" in cfg
        assert "schema.history.internal.kafka.topic" in cfg

    def test_schema_history_topic_name(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["schema.history.internal.kafka.topic"] == (
            "_schema-history.cdc.test-sqlserver"
        )

    def test_avro_converters(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert cfg["value.converter"] == "io.confluent.connect.avro.AvroConverter"

    def test_decimal_handling(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["decimal.handling.mode"] == "string"

    def test_snapshot_mode(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_sqlserver_connector_config(sqlserver_pipeline, PlatformConfig())
        assert cfg["snapshot.mode"] == "initial"

    def test_dispatch_to_sqlserver(self, sqlserver_pipeline: PipelineConfig):
        cfg = build_connector_config(sqlserver_pipeline, PlatformConfig())
        assert "SqlServerConnector" in cfg["connector.class"]


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

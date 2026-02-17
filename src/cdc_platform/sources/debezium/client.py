"""REST API wrapper for Kafka Connect (Debezium)."""

from __future__ import annotations

from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdc_platform.config.models import ConnectorConfig, PipelineConfig
from cdc_platform.sources.debezium.config import (
    build_postgres_connector_config,
    connector_name,
)

logger = structlog.get_logger()


class ConnectError(Exception):
    """Raised when a Kafka Connect API call fails."""


class DebeziumClient:
    """Thin async wrapper around the Kafka Connect REST API."""

    def __init__(self, config: ConnectorConfig | None = None) -> None:
        self._config = config or ConnectorConfig()
        self._client = httpx.AsyncClient(
            base_url=self._config.connect_url,
            timeout=self._config.timeout_seconds,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> DebeziumClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- Health ----------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, ConnectError)),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    async def wait_until_ready(self) -> None:
        """Block until Kafka Connect REST API is reachable."""
        resp = await self._client.get("/")
        resp.raise_for_status()
        logger.info("kafka_connect.ready", url=self._config.connect_url)

    # -- CRUD ------------------------------------------------------------------

    async def register_connector(self, pipeline: PipelineConfig) -> dict[str, Any]:
        """Idempotent PUT to register (or update) a connector."""
        name = connector_name(pipeline)
        config = build_postgres_connector_config(pipeline)
        resp = await self._client.put(
            f"/connectors/{name}/config",
            json=config,
        )
        if resp.status_code not in (200, 201):
            raise ConnectError(
                f"Failed to register connector {name}: "
                f"{resp.status_code} {resp.text}"
            )
        logger.info("connector.registered", connector=name)
        return resp.json()  # type: ignore[no-any-return]

    async def get_connector_status(self, name: str) -> dict[str, Any]:
        resp = await self._client.get(f"/connectors/{name}/status")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def list_connectors(self) -> list[str]:
        resp = await self._client.get("/connectors")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def delete_connector(self, name: str) -> None:
        resp = await self._client.delete(f"/connectors/{name}")
        resp.raise_for_status()
        logger.info("connector.deleted", connector=name)

    async def pause_connector(self, name: str) -> None:
        resp = await self._client.put(f"/connectors/{name}/pause")
        resp.raise_for_status()
        logger.info("connector.paused", connector=name)

    async def resume_connector(self, name: str) -> None:
        resp = await self._client.put(f"/connectors/{name}/resume")
        resp.raise_for_status()
        logger.info("connector.resumed", connector=name)

    async def restart_connector(self, name: str) -> None:
        resp = await self._client.post(
            f"/connectors/{name}/restart",
            params={"includeTasks": "true"},
        )
        resp.raise_for_status()
        logger.info("connector.restarted", connector=name)

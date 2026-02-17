"""Webhook (HTTP POST) sink connector."""

from __future__ import annotations

from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from cdc_platform.config.models import SinkConfig

logger = structlog.get_logger()


class WebhookSink:
    """Sends CDC events as JSON via HTTP POST (or configured method)."""

    def __init__(self, config: SinkConfig) -> None:
        self._config = config
        self._webhook = config.webhook
        if self._webhook is None:
            msg = "WebhookSink requires a webhook sub-config"
            raise ValueError(msg)
        self._client: httpx.AsyncClient | None = None
        self._flushed_offsets: dict[tuple[str, int], int] = {}

    @property
    def sink_id(self) -> str:
        return self._config.sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self._flushed_offsets

    async def start(self) -> None:
        headers: dict[str, str] = dict(self._webhook.headers)
        if self._webhook.auth_token is not None:
            headers["Authorization"] = (
                f"Bearer {self._webhook.auth_token.get_secret_value()}"
            )
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(self._webhook.timeout_seconds),
        )
        logger.info("webhook_sink.started", sink_id=self.sink_id, url=self._webhook.url)

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        if self._client is None:
            msg = "WebhookSink not started — call start() first"
            raise RuntimeError(msg)

        payload = {
            "key": key,
            "value": value,
            "metadata": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
            },
        }

        retry_cfg = self._config.retry

        @retry(
            stop=stop_after_attempt(retry_cfg.max_attempts),
            wait=wait_exponential_jitter(
                initial=retry_cfg.initial_wait_seconds,
                max=retry_cfg.max_wait_seconds,
                jitter=retry_cfg.multiplier if retry_cfg.jitter else 0,
            ),
            retry=retry_if_exception_type(
                (httpx.HTTPStatusError, httpx.TransportError)
            ),
            reraise=True,
        )
        async def _send() -> None:
            response = await self._client.request(
                method=self._webhook.method,
                url=self._webhook.url,
                json=payload,
            )
            response.raise_for_status()

        await _send()

        key_tp = (topic, partition)
        if offset > self._flushed_offsets.get(key_tp, -1):
            self._flushed_offsets[key_tp] = offset

        logger.debug(
            "webhook_sink.write",
            sink_id=self.sink_id,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    async def flush(self) -> None:
        # Webhook sink is not batched — no-op.
        pass

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.info("webhook_sink.stopped", sink_id=self.sink_id)

    async def health(self) -> dict[str, Any]:
        return {
            "sink_id": self.sink_id,
            "type": "webhook",
            "status": "running" if self._client is not None else "stopped",
            "url": self._webhook.url,
        }

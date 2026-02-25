"""Schema Registry monitor — polls for schema version changes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from typing import cast

import httpx
import structlog

logger = structlog.get_logger()


class SchemaMonitor:
    """Polls Schema Registry and logs schema version changes.

    When stop_on_incompatible=True, checks compatibility via the registry's
    /compatibility endpoint and sets incompatible_detected flag for the
    pipeline to act on.
    """

    def __init__(
        self,
        registry_url: str,
        topics: list[str],
        interval: float = 30.0,
        stop_on_incompatible: bool = False,
        on_incompatible: Callable[[], None] | None = None,
    ) -> None:
        self._registry_url = registry_url.rstrip("/")
        self._topics = topics
        self._interval = interval
        self._stop_on_incompatible = stop_on_incompatible
        self._on_incompatible = on_incompatible
        self._known_versions: dict[str, int] = {}
        self._known_schemas: dict[str, str] = {}
        self._incompatible_detected = False
        self._task: asyncio.Task | None = None  # type: ignore[type-arg]

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _poll_loop(self) -> None:
        while True:
            await self._check_schemas()
            await asyncio.sleep(self._interval)

    async def _check_schemas(self) -> None:
        async with httpx.AsyncClient() as client:
            for topic in self._topics:
                for suffix in ("-key", "-value"):
                    subject = f"{topic}{suffix}"
                    try:
                        resp = await client.get(
                            f"{self._registry_url}/subjects/{subject}/versions/latest"
                        )
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()
                        data = resp.json()
                        version = data["version"]
                        schema_id = data["id"]
                        schema_str = data.get("schema", "")

                        prev = self._known_versions.get(subject)
                        if prev is None:
                            self._known_versions[subject] = version
                            self._known_schemas[subject] = schema_str
                        elif version != prev:
                            compatible = True
                            if self._stop_on_incompatible and self._known_schemas.get(
                                subject
                            ):
                                compatible = await self._check_compatibility(
                                    client, subject, self._known_schemas[subject]
                                )

                            log_fn = logger.warning if not compatible else logger.info
                            log_fn(
                                "schema.version_changed",
                                subject=subject,
                                previous_version=prev,
                                new_version=version,
                                schema_id=schema_id,
                                compatible=compatible,
                            )

                            if not compatible:
                                self._incompatible_detected = True
                                logger.error(
                                    "schema.incompatible_change_detected",
                                    subject=subject,
                                    previous_version=prev,
                                    new_version=version,
                                )
                                if self._on_incompatible:
                                    self._on_incompatible()

                            self._known_versions[subject] = version
                            self._known_schemas[subject] = schema_str
                    except Exception as exc:
                        logger.warning(
                            "schema.check_failed", subject=subject, error=str(exc)
                        )

    async def _check_compatibility(
        self,
        client: httpx.AsyncClient,
        subject: str,
        old_schema: str,
    ) -> bool:
        """Check if old schema is compatible with latest using registry API."""
        try:
            resp = await client.post(
                f"{self._registry_url}/compatibility/subjects/{subject}/versions/latest",
                json={"schema": old_schema},
            )
            if resp.status_code == 200:
                return cast(bool, resp.json().get("is_compatible", True))
            return True
        except Exception:
            return True

    @property
    def known_versions(self) -> dict[str, int]:
        return dict(self._known_versions)

    @property
    def incompatible_detected(self) -> bool:
        return self._incompatible_detected

from __future__ import annotations

import atexit
import os
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from celery import Celery, signals

from celery_uptime.checks import HealthCheck, redis_check, sqs_check, unsupported_check
from celery_uptime.server import HealthState, UvicornHealthServer, create_health_app


@dataclass(frozen=True)
class MonitorConfig:
    """Runtime configuration for the embedded health server."""

    host: str = "0.0.0.0"
    port: int = 8090
    service: str | None = None
    log_level: str = "warning"
    enabled: bool = True

    @classmethod
    def from_env(cls) -> MonitorConfig:
        return cls(
            host=os.getenv("CELERY_UPTIME_HOST", "0.0.0.0"),
            port=int(os.getenv("CELERY_UPTIME_PORT", "8090")),
            service=os.getenv("CELERY_UPTIME_SERVICE"),
            log_level=os.getenv("CELERY_UPTIME_LOG_LEVEL", "warning"),
            enabled=_env_bool("CELERY_UPTIME_ENABLED", default=True),
        )


class CeleryUptimeMonitor:
    """Registers Celery signal handlers and owns the embedded HTTP server."""

    def __init__(
        self,
        celery_app: Celery,
        checks: Sequence[HealthCheck] | None = None,
        include_auto_checks: bool = True,
        config: MonitorConfig | None = None,
    ) -> None:
        self.celery_app = celery_app
        self.config = config or MonitorConfig.from_env()
        self._explicit_checks = list(checks or [])
        self._include_auto_checks = include_auto_checks
        self._server: UvicornHealthServer | None = None
        self._lock = threading.Lock()

    def register(self) -> CeleryUptimeMonitor:
        if not self.config.enabled:
            return self

        dispatch_uid = f"celery_uptime:{id(self)}"
        signals.worker_ready.connect(
            self._on_worker_ready,
            weak=False,
            dispatch_uid=f"{dispatch_uid}:worker_ready",
        )
        signals.beat_init.connect(
            self._on_beat_init,
            weak=False,
            dispatch_uid=f"{dispatch_uid}:beat_init",
        )
        signals.worker_shutdown.connect(
            self._on_worker_shutdown,
            weak=False,
            dispatch_uid=f"{dispatch_uid}:worker_shutdown",
        )
        atexit.register(self.stop)
        return self

    @property
    def server(self) -> UvicornHealthServer | None:
        return self._server

    def stop(self) -> None:
        with self._lock:
            if self._server is None:
                return
            self._server.stop()
            self._server = None

    def _on_worker_ready(self, **_: Any) -> None:
        self.start(process="worker")

    def _on_beat_init(self, **_: Any) -> None:
        self.start(process="beat")

    def _on_worker_shutdown(self, **_: Any) -> None:
        self.stop()

    def start(self, process: str) -> None:
        with self._lock:
            if self._server is not None and self._server.running:
                return

            service = self.config.service or f"{self.celery_app.main}-celery-{process}"
            state = HealthState(
                service=service,
                process=process,
                ready=True,
                checks=self._checks(),
            )
            app = create_health_app(state)
            self._server = UvicornHealthServer(
                app=app,
                host=self.config.host,
                port=self.config.port,
                log_level=self.config.log_level,
            )
            self._server.start()

    def _checks(self) -> list[HealthCheck]:
        checks = list(self._explicit_checks)
        if self._include_auto_checks:
            checks.extend(auto_checks(self.celery_app))
        return checks


def monitor(
    celery_app: Celery,
    *,
    checks: Sequence[HealthCheck] | None = None,
    include_auto_checks: bool = True,
    config: MonitorConfig | None = None,
) -> CeleryUptimeMonitor:
    """Attach an embedded health server to Celery worker and beat commands."""

    return CeleryUptimeMonitor(
        celery_app=celery_app,
        checks=checks,
        include_auto_checks=include_auto_checks,
        config=config,
    ).register()


def auto_checks(celery_app: Celery) -> list[HealthCheck]:
    return [
        _auto_broker_check(celery_app),
        _auto_backend_check(celery_app),
    ]


def _auto_broker_check(celery_app: Celery) -> HealthCheck:
    broker_url = _first_url(celery_app.conf.broker_url)
    if not broker_url:
        return unsupported_check("broker", "missing_config:broker_url")

    if broker_url.startswith("redis://") or broker_url.startswith("rediss://"):
        return redis_check("broker", broker_url)

    if broker_url.startswith("sqs://"):
        options = dict(celery_app.conf.broker_transport_options or {})
        queue_url = _sqs_queue_url(celery_app, options)
        access_key = options.get("aws_access_key_id") or options.get("access_key_id")
        secret_key = options.get("aws_secret_access_key") or options.get("secret_access_key")
        return sqs_check(
            "broker",
            endpoint_url=options.get("endpoint_url"),
            region=options.get("region"),
            queue_url=queue_url,
            access_key=access_key,
            secret_key=secret_key,
        )

    return unsupported_check("broker", f"unsupported_broker:{_scheme(broker_url)}")


def _auto_backend_check(celery_app: Celery) -> HealthCheck:
    backend_url = _first_url(celery_app.conf.result_backend)
    if not backend_url:
        return unsupported_check("backend", "missing_config:result_backend")

    if backend_url.startswith("redis://") or backend_url.startswith("rediss://"):
        return redis_check("backend", backend_url)

    if backend_url in {"disabled://", "rpc://"}:
        return unsupported_check("backend", f"unsupported_backend:{_scheme(backend_url)}")

    return unsupported_check("backend", f"unsupported_backend:{_scheme(backend_url)}")


def _sqs_queue_url(celery_app: Celery, options: dict[str, Any]) -> str | None:
    predefined_queues = options.get("predefined_queues") or {}
    default_queue = celery_app.conf.task_default_queue

    if default_queue in predefined_queues:
        queue_config = predefined_queues[default_queue] or {}
        return queue_config.get("url")

    if len(predefined_queues) == 1:
        queue_config = next(iter(predefined_queues.values())) or {}
        return queue_config.get("url")

    return options.get("queue_url")


def _first_url(value: str | Sequence[str] | None) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return next(iter(value), None)
    return None


def _scheme(url: str) -> str:
    return url.split(":", 1)[0] if ":" in url else "unknown"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() not in {"0", "false", "no", "off"}

from __future__ import annotations

import atexit
import os
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from celery import Celery, signals

from celery_uptime.checks import (
    HealthCheck,
    broker_connection_check,
    cassandra_check,
    database_check,
    disabled_backend_check,
    django_cache_check,
    django_database_check,
    elasticsearch_check,
    memcache_check,
    mongodb_check,
    redis_check,
    redis_sentinel_check,
    sqs_check,
    unsupported_check,
)
from celery_uptime.server import (
    DependencyProbeRunner,
    HealthState,
    ReadinessCache,
    UvicornHealthServer,
    create_health_app,
)

_MONITOR_ATTRIBUTE = "_celery_uptime_monitor"


@dataclass(frozen=True)
class MonitorConfig:
    """Runtime configuration for the embedded health server."""

    host: str = "0.0.0.0"
    port: int = 8090
    service: str | None = None
    log_level: str = "warning"
    enabled: bool = True
    check_interval: float = 30
    check_timeout: float = 5
    stale_after: float = 90

    @classmethod
    def from_env(cls) -> MonitorConfig:
        return cls(
            host=os.getenv("CELERY_UPTIME_HOST", "0.0.0.0"),
            port=int(os.getenv("CELERY_UPTIME_PORT", "8090")),
            service=os.getenv("CELERY_UPTIME_SERVICE"),
            log_level=os.getenv("CELERY_UPTIME_LOG_LEVEL", "warning"),
            enabled=_env_bool("CELERY_UPTIME_ENABLED", default=True),
            check_interval=float(os.getenv("CELERY_UPTIME_CHECK_INTERVAL", "30")),
            check_timeout=float(os.getenv("CELERY_UPTIME_CHECK_TIMEOUT", "5")),
            stale_after=float(os.getenv("CELERY_UPTIME_STALE_AFTER", "90")),
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
        self._probe_runner: DependencyProbeRunner | None = None
        self._state: HealthState | None = None
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
            if self._state is not None:
                self._state.ready = False
            if self._probe_runner is not None:
                self._probe_runner.stop()
                self._probe_runner = None
            if self._server is None:
                return
            self._server.stop()
            self._server = None

    def _on_worker_ready(self, sender: Any = None, **_: Any) -> None:
        self.start(process="worker", worker=_worker_info(sender))

    def _on_beat_init(self, **_: Any) -> None:
        self.start(process="beat")

    def _on_worker_shutdown(self, **_: Any) -> None:
        self.stop()

    def start(self, process: str, worker: dict[str, object] | None = None) -> None:
        with self._lock:
            if self._server is not None and self._server.running:
                return

            service = self.config.service or f"{self.celery_app.main}-celery-{process}"
            checks = self._checks()
            readiness = ReadinessCache(stale_after=self.config.stale_after)
            self._state = HealthState(
                service=service,
                process=process,
                ready=True,
                readiness=readiness,
                worker=worker,
            )
            app = create_health_app(self._state)
            self._server = UvicornHealthServer(
                app=app,
                host=self.config.host,
                port=self.config.port,
                log_level=self.config.log_level,
            )
            self._probe_runner = DependencyProbeRunner(
                checks=checks,
                cache=readiness,
                interval=self.config.check_interval,
                timeout=self.config.check_timeout,
            )
            self._server.start()
            self._probe_runner.start()

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

    existing = getattr(celery_app, _MONITOR_ATTRIBUTE, None)
    if isinstance(existing, CeleryUptimeMonitor):
        return existing

    uptime = CeleryUptimeMonitor(
        celery_app=celery_app,
        checks=checks,
        include_auto_checks=include_auto_checks,
        config=config,
    )
    setattr(celery_app, _MONITOR_ATTRIBUTE, uptime)
    return uptime.register()


def auto_checks(celery_app: Celery) -> list[HealthCheck]:
    return [
        _auto_broker_check(celery_app),
        _auto_backend_check(celery_app),
    ]


def _auto_broker_check(celery_app: Celery) -> HealthCheck:
    broker_url = _first_url(celery_app.conf.broker_url)
    if not broker_url:
        return unsupported_check("broker", "missing_config:broker_url")

    scheme = _scheme(broker_url)

    if scheme in {"amqp", "pyamqp", "librabbitmq", "kafka", "confluentkafka"}:
        return broker_connection_check("broker", celery_app)

    if scheme in {"redis", "rediss"}:
        return redis_check("broker", broker_url)

    if scheme == "sentinel":
        options = dict(celery_app.conf.broker_transport_options or {})
        return redis_sentinel_check(
            "broker",
            _urls(celery_app.conf.broker_url),
            master_name=options.get("master_name"),
            sentinel_kwargs=options.get("sentinel_kwargs"),
        )

    if scheme == "sqs":
        options = dict(celery_app.conf.broker_transport_options or {})
        queue_config = _sqs_queue_config(celery_app, options)
        return sqs_check(
            "broker",
            endpoint_url=options.get("endpoint_url"),
            region=options.get("region"),
            queue_url=queue_config.get("url") or options.get("queue_url"),
            access_key=(
                queue_config.get("aws_access_key_id")
                or queue_config.get("access_key_id")
                or options.get("aws_access_key_id")
                or options.get("access_key_id")
            ),
            secret_key=(
                queue_config.get("aws_secret_access_key")
                or queue_config.get("secret_access_key")
                or options.get("aws_secret_access_key")
                or options.get("secret_access_key")
            ),
        )

    return unsupported_check("broker", f"unsupported_broker:{scheme}")


def _auto_backend_check(celery_app: Celery) -> HealthCheck:
    backend_url = _first_url(celery_app.conf.result_backend)
    if not backend_url:
        return disabled_backend_check("backend")

    scheme = _backend_scheme(backend_url)

    if scheme == "disabled":
        return disabled_backend_check("backend")

    if scheme in {"redis", "rediss"}:
        return redis_check("backend", backend_url)

    if scheme == "sentinel":
        options = dict(getattr(celery_app.conf, "result_backend_transport_options", None) or {})
        return redis_sentinel_check(
            "backend",
            _urls(celery_app.conf.result_backend),
            master_name=options.get("master_name"),
            sentinel_kwargs=options.get("sentinel_kwargs"),
        )

    if scheme in {"db", "database"}:
        return database_check("backend", backend_url)

    if scheme == "django-db":
        return django_database_check("backend")

    if scheme == "django-cache":
        return django_cache_check("backend")

    if scheme == "rpc":
        return broker_connection_check("backend", celery_app, required=False, success_detail="rpc_via_broker")

    if scheme == "cache" and _cache_backend_kind(backend_url) in {"memcached", "pymemcache", "pylibmc"}:
        return memcache_check("backend", backend_url)

    if scheme in {"mongodb", "mongodb+srv"}:
        return mongodb_check("backend", backend_url)

    if scheme == "elasticsearch":
        return elasticsearch_check("backend", backend_url)

    if scheme == "cassandra":
        return cassandra_check("backend", backend_url)

    return unsupported_check("backend", f"unsupported_backend:{scheme}")


def _sqs_queue_config(celery_app: Celery, options: dict[str, Any]) -> dict[str, Any]:
    predefined_queues = options.get("predefined_queues") or {}
    default_queue = celery_app.conf.task_default_queue

    if default_queue in predefined_queues:
        return predefined_queues[default_queue] or {}

    if len(predefined_queues) == 1:
        return next(iter(predefined_queues.values())) or {}

    return {}


def _first_url(value: str | Sequence[str] | None) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return next(iter(value), None)
    return None


def _urls(value: str | Sequence[str] | None) -> str | list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return list(value)


def _scheme(url: str) -> str:
    return url.split(":", 1)[0] if ":" in url else "unknown"


def _backend_scheme(url: str) -> str:
    if "://" not in url:
        return url
    scheme = _scheme(url)
    if "+" in scheme:
        return scheme.split("+", 1)[0]
    return scheme


def _cache_backend_kind(url: str) -> str:
    scheme = _scheme(url)
    if "+" not in scheme:
        return ""
    return scheme.split("+", 1)[1]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() not in {"0", "false", "no", "off"}


def _worker_info(sender: Any) -> dict[str, object]:
    controller = getattr(sender, "controller", None)
    pool = getattr(sender, "pool", None) or getattr(controller, "pool", None)
    concurrency = getattr(controller, "concurrency", None)
    if concurrency is None and pool is not None:
        concurrency = getattr(pool, "limit", None)

    return {
        "ready_at": time.time(),
        "pool": type(pool).__name__ if pool is not None else None,
        "concurrency": concurrency,
    }

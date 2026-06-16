from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from queue import Queue

import uvicorn
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from celery_uptime.checks import CheckResult, HealthCheck, check_payload


@dataclass
class HealthState:
    service: str
    process: str
    ready: bool
    readiness: ReadinessCache
    worker: dict[str, object] | None = None


@dataclass(frozen=True)
class ReadinessSnapshot:
    checked_at: float | None
    duration_seconds: float | None
    checks: dict[str, dict[str, object]]


class ReadinessCache:
    def __init__(self, stale_after: float) -> None:
        self._stale_after = stale_after
        self._snapshot = ReadinessSnapshot(
            checked_at=None,
            duration_seconds=None,
            checks={},
        )
        self._lock = threading.Lock()

    def update(
        self,
        results: Sequence[CheckResult],
        duration_seconds: float,
        checked_at: float | None = None,
    ) -> None:
        snapshot = ReadinessSnapshot(
            checked_at=checked_at or time.time(),
            duration_seconds=duration_seconds,
            checks={result.name: check_payload(result) for result in results},
        )
        with self._lock:
            self._snapshot = snapshot

    def snapshot(self) -> ReadinessSnapshot:
        with self._lock:
            return self._snapshot

    def age_seconds(self, now: float | None = None) -> float | None:
        snapshot = self.snapshot()
        if snapshot.checked_at is None:
            return None
        return (now or time.time()) - snapshot.checked_at

    def is_stale(self, now: float | None = None) -> bool:
        age = self.age_seconds(now)
        return age is not None and age > self._stale_after


class DependencyProbeRunner:
    def __init__(
        self,
        checks: Sequence[HealthCheck],
        cache: ReadinessCache,
        interval: float,
        timeout: float,
    ) -> None:
        self._checks = list(checks)
        self._cache = cache
        self._interval = interval
        self._timeout = timeout
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="celery-uptime-probes",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def probe_once(self) -> None:
        start = time.monotonic()
        checked_at = time.time()
        results = [self._run_check(check) for check in self._checks]
        self._cache.update(
            results=results,
            duration_seconds=time.monotonic() - start,
            checked_at=checked_at,
        )

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.probe_once()
            self._stop_event.wait(self._interval)

    def _run_check(self, check: HealthCheck) -> CheckResult:
        results: Queue[CheckResult | Exception] = Queue(maxsize=1)

        def target() -> None:
            try:
                results.put(check())
            except Exception as exc:
                results.put(exc)

        thread = threading.Thread(
            target=target,
            name=f"celery-uptime-check-{_check_name(check)}",
            daemon=True,
        )
        thread.start()
        thread.join(timeout=self._timeout)
        if thread.is_alive():
            return CheckResult(name=_check_name(check), ok=False, detail="timeout")

        result = results.get_nowait()
        if isinstance(result, CheckResult):
            return result
        return CheckResult(name=_check_name(check), ok=False, detail=str(result))


def create_health_app(state: HealthState) -> FastAPI:
    app = FastAPI(title="celery uptime")

    @app.get("/health")
    def health() -> JSONResponse:
        return JSONResponse(
            content={
                "status": "ok" if state.ready else "error",
                "service": state.service,
                "process": state.process,
                "worker": state.worker,
            },
            status_code=status.HTTP_200_OK if state.ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    @app.get("/ready")
    def ready() -> JSONResponse:
        snapshot = state.readiness.snapshot()
        checks = {
            "celery_process": {
                "status": "ok" if state.ready else "error",
                "detail": "ready" if state.ready else "not_ready",
            },
        }
        checks.update(snapshot.checks)
        detail = "ok"
        if snapshot.checked_at is None:
            detail = "not_checked_yet"
            is_ready = False
        elif state.readiness.is_stale():
            detail = "stale"
            is_ready = False
        else:
            is_ready = state.ready and all(
                check.get("status") == "ok" or check.get("required") is False
                for check in checks.values()
            )

        return JSONResponse(
            content={
                "status": "ok" if is_ready else "error",
                "service": state.service,
                "process": state.process,
                "detail": detail,
                "checked_at": snapshot.checked_at,
                "age_seconds": state.readiness.age_seconds(),
                "duration_seconds": snapshot.duration_seconds,
                "worker": state.worker,
                "checks": checks,
            },
            status_code=status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    return app


class UvicornHealthServer:
    def __init__(
        self,
        app: FastAPI,
        host: str,
        port: int,
        log_level: str,
    ) -> None:
        self._config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level=log_level,
            lifespan="off",
        )
        self._server = uvicorn.Server(self._config)
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return

        self._thread = threading.Thread(
            target=self._server.run,
            name="celery-uptime",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._server.should_exit = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)


def _check_name(check: HealthCheck) -> str:
    result = getattr(check, "__celery_uptime_name__", None)
    if isinstance(result, str):
        return result
    return getattr(check, "__name__", "check")

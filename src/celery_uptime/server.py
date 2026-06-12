from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from celery_uptime.checks import HealthCheck, check_payload


@dataclass
class HealthState:
    service: str
    process: str
    ready: bool
    checks: Sequence[HealthCheck]


def create_health_app(state: HealthState) -> FastAPI:
    app = FastAPI(title="celery uptime")

    @app.get("/health")
    def health() -> JSONResponse:
        return JSONResponse(
            content={
                "status": "ok" if state.ready else "error",
                "service": state.service,
                "process": state.process,
            },
            status_code=status.HTTP_200_OK if state.ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    @app.get("/ready")
    def ready() -> JSONResponse:
        results = [check() for check in state.checks]
        checks = {
            "celery_process": {
                "status": "ok" if state.ready else "error",
                "detail": "ready" if state.ready else "not_ready",
            },
        }
        checks.update({result.name: check_payload(result) for result in results})
        is_ready = state.ready and all(result.ok or not result.required for result in results)

        return JSONResponse(
            content={
                "status": "ok" if is_ready else "error",
                "service": state.service,
                "process": state.process,
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

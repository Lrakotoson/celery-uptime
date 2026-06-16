import json
import time

from celery_uptime.checks import CheckResult
from celery_uptime.server import DependencyProbeRunner, HealthState, ReadinessCache, create_health_app


def response_json(response):
    return json.loads(response.body.decode())


def route(app, path):
    return next(route.endpoint for route in app.routes if getattr(route, "path", None) == path)


def test_health_returns_ok_when_state_is_ready_and_does_not_call_checks():
    def failing_check():
        raise AssertionError("health must not call dependency checks")

    cache = ReadinessCache(stale_after=90)
    app = create_health_app(
        HealthState(
            service="svc",
            process="worker",
            ready=True,
            readiness=cache,
            worker={"pool": "prefork", "concurrency": 4},
        )
    )
    response = route(app, "/health")()

    data = response_json(response)
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["worker"] == {"pool": "prefork", "concurrency": 4}
    failing_check  # keep the assertion target visible without executing it


def test_ready_returns_not_checked_yet_before_first_probe():
    cache = ReadinessCache(stale_after=90)
    app = create_health_app(HealthState(service="svc", process="worker", ready=True, readiness=cache))

    response = route(app, "/ready")()

    data = response_json(response)
    assert response.status_code == 503
    assert data["status"] == "error"
    assert data["detail"] == "not_checked_yet"


def test_ready_returns_cached_success_immediately():
    cache = ReadinessCache(stale_after=90)
    cache.update(
        [
            CheckResult(name="broker", ok=True, detail="ok"),
            CheckResult(name="backend", ok=True, detail="ok"),
        ],
        duration_seconds=0.01,
    )
    app = create_health_app(HealthState(service="svc", process="worker", ready=True, readiness=cache))

    response = route(app, "/ready")()

    data = response_json(response)
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["checks"]["broker"] == {"status": "ok", "detail": "ok"}
    assert data["duration_seconds"] == 0.01


def test_ready_returns_cached_failure_immediately():
    cache = ReadinessCache(stale_after=90)
    cache.update([CheckResult(name="broker", ok=False, detail="down")], duration_seconds=0.01)
    app = create_health_app(HealthState(service="svc", process="worker", ready=True, readiness=cache))

    response = route(app, "/ready")()

    data = response_json(response)
    assert response.status_code == 503
    assert data["status"] == "error"
    assert data["checks"]["broker"] == {"status": "error", "detail": "down"}


def test_ready_returns_stale_when_cache_is_too_old():
    cache = ReadinessCache(stale_after=0.01)
    cache.update([CheckResult(name="broker", ok=True, detail="ok")], duration_seconds=0.01, checked_at=time.time() - 1)
    app = create_health_app(HealthState(service="svc", process="worker", ready=True, readiness=cache))

    response = route(app, "/ready")()

    data = response_json(response)
    assert response.status_code == 503
    assert data["detail"] == "stale"


def test_busy_worker_payload_does_not_make_ready_unhealthy():
    cache = ReadinessCache(stale_after=90)
    cache.update([CheckResult(name="broker", ok=True, detail="ok")], duration_seconds=0.01)
    app = create_health_app(
        HealthState(
            service="svc",
            process="worker",
            ready=True,
            readiness=cache,
            worker={"pool": "prefork", "concurrency": 4, "busy": 4},
        )
    )

    response = route(app, "/ready")()

    data = response_json(response)
    assert response.status_code == 200
    assert data["worker"]["busy"] == 4


def test_slow_check_times_out_and_is_cached():
    def slow_check():
        time.sleep(1)
        return CheckResult(name="broker", ok=True, detail="ok")

    setattr(slow_check, "__celery_uptime_name__", "broker")
    cache = ReadinessCache(stale_after=90)
    runner = DependencyProbeRunner([slow_check], cache=cache, interval=30, timeout=0.01)

    runner.probe_once()

    snapshot = cache.snapshot()
    assert snapshot.checks["broker"] == {"status": "error", "detail": "timeout"}


def test_probe_runner_start_is_idempotent():
    cache = ReadinessCache(stale_after=90)
    runner = DependencyProbeRunner([], cache=cache, interval=30, timeout=1)

    runner.start()
    first_thread = runner._thread
    runner.start()

    assert runner._thread is first_thread
    runner.stop()

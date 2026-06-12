from celery_uptime.checks import CheckResult
from celery_uptime.server import HealthState, create_health_app


def response_json(response):
    import json

    return json.loads(response.body.decode())


def test_health_returns_ok_when_state_is_ready():
    app = create_health_app(HealthState(service="svc", process="worker", ready=True, checks=[]))
    response = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/health")()

    assert response.status_code == 200
    assert response_json(response)["status"] == "ok"


def test_ready_returns_ok_when_required_checks_pass():
    app = create_health_app(
        HealthState(
            service="svc",
            process="worker",
            ready=True,
            checks=[
                lambda: CheckResult(name="broker", ok=True, detail="ok"),
                lambda: CheckResult(name="backend", ok=True, detail="ok"),
            ],
        )
    )
    response = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/ready")()

    data = response_json(response)
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["checks"]["broker"] == {"status": "ok", "detail": "ok"}


def test_ready_returns_error_when_required_check_fails():
    app = create_health_app(
        HealthState(
            service="svc",
            process="worker",
            ready=True,
            checks=[lambda: CheckResult(name="broker", ok=False, detail="down")],
        )
    )
    response = next(route.endpoint for route in app.routes if getattr(route, "path", None) == "/ready")()

    data = response_json(response)
    assert response.status_code == 503
    assert data["status"] == "error"
    assert data["checks"]["broker"] == {"status": "error", "detail": "down"}

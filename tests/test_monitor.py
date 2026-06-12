from importlib import import_module

from celery import Celery

from celery_uptime.checks import CheckResult
from celery_uptime.monitor import MonitorConfig, auto_checks, monitor

monitor_module = import_module("celery_uptime.monitor")


class FakeServer:
    starts = 0
    stops = 0

    def __init__(self, *_args, **_kwargs):
        self.running = False

    def start(self):
        type(self).starts += 1
        self.running = True

    def stop(self):
        type(self).stops += 1
        self.running = False


def passing_check(name="broker"):
    return lambda: CheckResult(name=name, ok=True, detail="ok")


def test_monitor_registers_without_starting_server(monkeypatch):
    FakeServer.starts = 0
    monkeypatch.setattr(monitor_module, "UvicornHealthServer", FakeServer)
    app = Celery("test", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")

    monitor(app, checks=[passing_check()], include_auto_checks=False)

    assert FakeServer.starts == 0


def test_worker_ready_starts_one_server(monkeypatch):
    FakeServer.starts = 0
    monkeypatch.setattr(monitor_module, "UvicornHealthServer", FakeServer)
    app = Celery("test", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    uptime = monitor(
        app,
        checks=[passing_check()],
        include_auto_checks=False,
        config=MonitorConfig(port=8091),
    )

    uptime.start("worker")
    uptime.start("worker")

    assert FakeServer.starts == 1


def test_beat_start_uses_beat_process(monkeypatch):
    seen = {}

    def fake_create_health_app(state):
        seen["process"] = state.process
        return object()

    monkeypatch.setattr(monitor_module, "create_health_app", fake_create_health_app)
    monkeypatch.setattr(monitor_module, "UvicornHealthServer", FakeServer)
    app = Celery("test", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    uptime = monitor(app, checks=[passing_check()], include_auto_checks=False)

    uptime.start("beat")

    assert seen["process"] == "beat"


def test_worker_shutdown_stops_server(monkeypatch):
    FakeServer.starts = 0
    FakeServer.stops = 0
    monkeypatch.setattr(monitor_module, "UvicornHealthServer", FakeServer)
    app = Celery("test", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    uptime = monitor(app, checks=[passing_check()], include_auto_checks=False)

    uptime.start("worker")
    uptime.stop()

    assert FakeServer.stops == 1


def test_disabled_monitor_does_not_register(monkeypatch):
    connected = []

    def fake_connect(*args, **kwargs):
        connected.append((args, kwargs))

    monkeypatch.setattr(monitor_module.signals.worker_ready, "connect", fake_connect)
    monkeypatch.setattr(monitor_module.signals.beat_init, "connect", fake_connect)
    monkeypatch.setattr(monitor_module.signals.worker_shutdown, "connect", fake_connect)
    app = Celery("test", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1")
    monitor(app, config=MonitorConfig(enabled=False))

    assert connected == []


def test_auto_checks_include_sqs_broker_and_redis_backend(monkeypatch):
    created_checks = []

    def fake_sqs_check(name, **_kwargs):
        created_checks.append(name)
        return lambda: CheckResult(name=name, ok=True, detail="ok")

    monkeypatch.setattr(monitor_module, "sqs_check", fake_sqs_check)
    app = Celery("test", broker="sqs://", backend="redis://localhost:6379/1")
    app.conf.broker_transport_options = {
        "endpoint_url": "https://sqs.example",
        "region": "fr-par",
        "aws_access_key_id": "access",
        "aws_secret_access_key": "secret",
        "predefined_queues": {
            "celery": {
                "url": "https://sqs.example/queue",
            },
        },
    }

    checks = auto_checks(app)

    assert len(checks) == 2
    assert created_checks == ["broker"]
    assert checks[1]().name == "backend"


def test_auto_checks_fail_closed_for_unsupported_broker():
    app = Celery("test", broker="amqp://guest:guest@localhost:5672//", backend="redis://localhost:6379/1")

    result = auto_checks(app)[0]()

    assert result.name == "broker"
    assert result.ok is False
    assert result.detail == "unsupported_broker:amqp"

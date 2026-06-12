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


def fake_check(name, detail="ok"):
    return lambda: CheckResult(name=name, ok=True, detail=detail)


def test_auto_checks_include_sqs_broker_and_redis_backend(monkeypatch):
    created_checks = []

    def fake_sqs_check(name, **_kwargs):
        created_checks.append(name)
        return fake_check(name)

    monkeypatch.setattr(monitor_module, "sqs_check", fake_sqs_check)
    monkeypatch.setattr(monitor_module, "redis_check", lambda name, *_args, **_kwargs: fake_check(name))
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


def test_auto_checks_include_generic_rabbitmq_broker(monkeypatch):
    monkeypatch.setattr(monitor_module, "broker_connection_check", lambda name, *_args, **_kwargs: fake_check(name))
    app = Celery("test", broker="pyamqp://guest:guest@localhost//", backend=None)

    result = auto_checks(app)[0]()

    assert result.name == "broker"
    assert result.ok is True


def test_auto_checks_include_kafka_broker(monkeypatch):
    monkeypatch.setattr(monitor_module, "broker_connection_check", lambda name, *_args, **_kwargs: fake_check(name))
    app = Celery("test", broker="kafka://localhost:9092", backend=None)

    result = auto_checks(app)[0]()

    assert result.name == "broker"
    assert result.ok is True


def test_auto_checks_include_redis_sentinel_broker(monkeypatch):
    seen = {}

    def fake_sentinel_check(name, urls, master_name, **_kwargs):
        seen["urls"] = urls
        seen["master_name"] = master_name
        return fake_check(name)

    monkeypatch.setattr(monitor_module, "redis_sentinel_check", fake_sentinel_check)
    app = Celery("test", broker="sentinel://localhost:26379/0", backend=None)
    app.conf.broker_transport_options = {"master_name": "mymaster"}

    result = auto_checks(app)[0]()

    assert result.ok is True
    assert seen == {"urls": "sentinel://localhost:26379/0", "master_name": "mymaster"}


def test_auto_checks_treat_absent_backend_as_disabled():
    app = Celery("test", broker="redis://localhost:6379/0", backend=None)

    result = auto_checks(app)[1]()

    assert result.name == "backend"
    assert result.ok is True
    assert result.required is False
    assert result.detail == "disabled"


def test_auto_checks_include_database_backend(monkeypatch):
    monkeypatch.setattr(monitor_module, "database_check", lambda name, *_args, **_kwargs: fake_check(name))
    app = Celery("test", broker="redis://localhost:6379/0", backend="db+sqlite:///results.sqlite")

    assert auto_checks(app)[1]().ok is True


def test_auto_checks_include_django_backends(monkeypatch):
    monkeypatch.setattr(monitor_module, "django_database_check", lambda name, **_kwargs: fake_check(name))
    monkeypatch.setattr(monitor_module, "django_cache_check", lambda name, **_kwargs: fake_check(name))

    assert auto_checks(Celery("test", broker="redis://localhost:6379/0", backend="django-db"))[1]().ok is True
    assert auto_checks(Celery("test", broker="redis://localhost:6379/0", backend="django-cache"))[1]().ok is True


def test_auto_checks_include_rpc_backend(monkeypatch):
    monkeypatch.setattr(
        monitor_module,
        "broker_connection_check",
        lambda name, *_args, **_kwargs: fake_check(name, detail="rpc_via_broker"),
    )
    app = Celery("test", broker="pyamqp://guest:guest@localhost//", backend="rpc://")

    result = auto_checks(app)[1]()

    assert result.name == "backend"
    assert result.detail == "rpc_via_broker"


def test_auto_checks_include_memcache_backend(monkeypatch):
    monkeypatch.setattr(monitor_module, "memcache_check", lambda name, *_args, **_kwargs: fake_check(name))
    app = Celery("test", broker="redis://localhost:6379/0", backend="cache+memcached://127.0.0.1:11211/")

    assert auto_checks(app)[1]().ok is True


def test_auto_checks_include_rare_backends(monkeypatch):
    monkeypatch.setattr(monitor_module, "mongodb_check", lambda name, *_args, **_kwargs: fake_check(name))
    monkeypatch.setattr(monitor_module, "elasticsearch_check", lambda name, *_args, **_kwargs: fake_check(name))
    monkeypatch.setattr(monitor_module, "cassandra_check", lambda name, *_args, **_kwargs: fake_check(name))

    assert auto_checks(Celery("test", broker="redis://localhost:6379/0", backend="mongodb://localhost/celery"))[1]().ok
    assert auto_checks(
        Celery("test", broker="redis://localhost:6379/0", backend="elasticsearch://localhost:9200/index/doc")
    )[1]().ok
    assert auto_checks(Celery("test", broker="redis://localhost:6379/0", backend="cassandra://localhost/keyspace"))[
        1
    ]().ok


def test_auto_checks_fail_closed_for_unsupported_broker():
    app = Celery("test", broker="filesystem://", backend="redis://localhost:6379/1")

    result = auto_checks(app)[0]()

    assert result.name == "broker"
    assert result.ok is False
    assert result.detail == "unsupported_broker:filesystem"

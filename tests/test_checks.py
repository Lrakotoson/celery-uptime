from types import SimpleNamespace

from celery_uptime import checks
from celery_uptime.checks import (
    CheckResult,
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
)


def test_redis_check_reports_missing_url():
    result = redis_check("backend", None)()

    assert result.name == "backend"
    assert result.ok is False
    assert result.detail == "missing_config:url"


def test_redis_check_reports_missing_extra(monkeypatch):
    monkeypatch.setattr(
        checks,
        "_lazy_import",
        lambda *_args: (_ for _ in ()).throw(checks.MissingExtraError("redis")),
    )

    result = redis_check("backend", "redis://localhost:6379/0")()

    assert result.ok is False
    assert result.detail == "missing_extra:redis"


def test_redis_check_maps_client_exceptions(monkeypatch):
    class Client:
        def ping(self):
            raise ConnectionError("redis down")

        def close(self):
            pass

    fake_redis = SimpleNamespace(from_url=lambda *_args, **_kwargs: Client())
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_redis)

    result = redis_check("backend", "redis://localhost:6379/0")()

    assert result.ok is False
    assert result.detail == "redis down"


def test_redis_sentinel_check_requires_master_name():
    result = redis_sentinel_check("broker", "sentinel://localhost:26379/0", None)()

    assert result.ok is False
    assert result.detail == "missing_config:master_name"


def test_sqs_check_reports_missing_config():
    result = sqs_check("broker", None, "fr-par", None, "access", "secret")()

    assert result.name == "broker"
    assert result.ok is False
    assert result.detail == "missing_config:endpoint_url,queue_url"


def test_sqs_check_maps_client_exceptions(monkeypatch):
    class BotoCoreError(Exception):
        pass

    class ClientError(Exception):
        pass

    class Client:
        def get_queue_attributes(self, **_kwargs):
            raise ClientError("missing queue")

    def fake_import(module, _extra):
        if module == "boto3":
            return SimpleNamespace(client=lambda *_args, **_kwargs: Client())
        if module == "botocore.config":
            return SimpleNamespace(Config=lambda **_kwargs: object())
        if module == "botocore.exceptions":
            return SimpleNamespace(BotoCoreError=BotoCoreError, ClientError=ClientError)
        raise AssertionError(module)

    monkeypatch.setattr(checks, "_lazy_import", fake_import)

    result = sqs_check(
        "broker",
        endpoint_url="https://sqs.example",
        region="fr-par",
        queue_url="https://sqs.example/queue",
        access_key="access",
        secret_key="secret",
    )()

    assert result.ok is False
    assert result.detail == "missing queue"


def test_database_check_uses_sqlalchemy(monkeypatch):
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, statement):
            assert statement == "SELECT 1"

    class Engine:
        def connect(self):
            return Connection()

        def dispose(self):
            pass

    fake_sqlalchemy = SimpleNamespace(create_engine=lambda url: Engine(), text=lambda value: value)
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_sqlalchemy)

    result = database_check("backend", "db+sqlite:///results.sqlite")()

    assert result == CheckResult(name="backend", ok=True, detail="ok", required=True)


def test_django_database_check(monkeypatch):
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, statement):
            assert statement == "SELECT 1"

        def fetchone(self):
            return (1,)

    fake_django_db = SimpleNamespace(connection=SimpleNamespace(cursor=lambda: Cursor()))
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_django_db)

    assert django_database_check("backend")().ok is True


def test_django_cache_check(monkeypatch):
    cache = SimpleNamespace(get=lambda key: None)
    fake_django_cache = SimpleNamespace(caches={"default": cache})
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_django_cache)

    assert django_cache_check("backend")().ok is True


def test_mongodb_check(monkeypatch):
    class Client:
        admin = SimpleNamespace(command=lambda command: {"ok": 1})

        def close(self):
            pass

    fake_pymongo = SimpleNamespace(MongoClient=lambda *_args, **_kwargs: Client())
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_pymongo)

    assert mongodb_check("backend", "mongodb://localhost/celery")().ok is True


def test_elasticsearch_check(monkeypatch):
    class Client:
        def ping(self):
            return True

        def close(self):
            pass

    fake_elasticsearch = SimpleNamespace(Elasticsearch=lambda *_args, **_kwargs: Client())
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_elasticsearch)

    assert elasticsearch_check("backend", "elasticsearch://localhost:9200/index/doc")().ok is True


def test_cassandra_check(monkeypatch):
    class Session:
        def execute(self, statement):
            assert statement == "SELECT now() FROM system.local"

    class Cluster:
        def __init__(self, hosts):
            assert hosts == ["localhost"]

        def connect(self):
            return Session()

        def shutdown(self):
            pass

    fake_cassandra = SimpleNamespace(Cluster=Cluster)
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_cassandra)

    assert cassandra_check("backend", "cassandra://localhost/keyspace")().ok is True


def test_memcache_check(monkeypatch):
    class Client:
        def __init__(self, servers):
            assert servers == ["127.0.0.1:11211"]

        def get_stats(self, *_args):
            return [("127.0.0.1:11211", {"version": "1.6"})]

        def disconnect_all(self):
            pass

    fake_memcache = SimpleNamespace(Client=Client)
    monkeypatch.setattr(checks, "_lazy_import", lambda *_args: fake_memcache)

    assert memcache_check("backend", "cache+memcached://127.0.0.1:11211/")().ok is True


def test_disabled_backend_check_is_non_required_ok():
    result = disabled_backend_check("backend")()

    assert result.ok is True
    assert result.required is False
    assert result.detail == "disabled"

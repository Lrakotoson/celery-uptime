from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from celery import Celery


@dataclass(frozen=True)
class CheckResult:
    """Result returned by a readiness check."""

    name: str
    ok: bool
    detail: str
    required: bool = True

    @property
    def status(self) -> str:
        return "ok" if self.ok else "error"


HealthCheck = Callable[[], CheckResult]


def broker_connection_check(
    name: str,
    celery_app: Celery,
    required: bool = True,
    success_detail: str = "ok",
) -> HealthCheck:
    """Create a generic Kombu broker connection check."""

    def check() -> CheckResult:
        try:
            connection = celery_app.connection_for_read()
            try:
                connection.ensure_connection(max_retries=1)
            finally:
                connection.release()
        except ModuleNotFoundError as exc:
            return CheckResult(
                name=name,
                ok=not required,
                detail=f"missing_extra:{_extra_from_module(exc.name)}",
                required=required,
            )
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail=success_detail, required=required)

    return _named_check(name, check)


def redis_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create a Redis ping health check."""

    def check() -> CheckResult:
        if not url or "None" in url:
            return CheckResult(
                name=name,
                ok=not required,
                detail="missing_config:url",
                required=required,
            )

        try:
            redis = _lazy_import("redis", "redis")
            client = redis.from_url(url, decode_responses=True)
            try:
                client.ping()
            finally:
                client.close()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def redis_sentinel_check(
    name: str,
    urls: str | list[str] | tuple[str, ...] | None,
    master_name: str | None,
    required: bool = True,
    sentinel_kwargs: dict[str, Any] | None = None,
) -> HealthCheck:
    """Create a Redis Sentinel master ping health check."""

    def check() -> CheckResult:
        if not urls:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)
        if not master_name:
            return CheckResult(name=name, ok=not required, detail="missing_config:master_name", required=required)

        try:
            redis_sentinel = _lazy_import("redis.sentinel", "redis")
            sentinel = redis_sentinel.Sentinel(
                _sentinel_hosts(urls),
                socket_timeout=2,
                sentinel_kwargs=sentinel_kwargs or {},
            )
            client = sentinel.master_for(master_name)
            client.ping()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def sqs_check(
    name: str,
    endpoint_url: str | None,
    region: str | None,
    queue_url: str | None,
    access_key: str | None,
    secret_key: str | None,
    required: bool = True,
) -> HealthCheck:
    """Create an SQS queue attributes health check."""

    def check() -> CheckResult:
        missing = [
            missing_name
            for missing_name, value in {
                "endpoint_url": endpoint_url,
                "region": region,
                "queue_url": queue_url,
                "access_key": access_key,
                "secret_key": secret_key,
            }.items()
            if not value
        ]
        if missing:
            return CheckResult(
                name=name,
                ok=not required,
                detail=f"missing_config:{','.join(missing)}",
                required=required,
            )

        try:
            boto3 = _lazy_import("boto3", "sqs")
            botocore_config = _lazy_import("botocore.config", "sqs")
            botocore_exceptions = _lazy_import("botocore.exceptions", "sqs")
            client = boto3.client(
                "sqs",
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botocore_config.Config(
                    connect_timeout=2,
                    read_timeout=3,
                    retries={"max_attempts": 1},
                ),
            )
            client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn"],
            )
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except (botocore_exceptions.BotoCoreError, botocore_exceptions.ClientError) as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def database_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create a SQLAlchemy database connection check."""

    def check() -> CheckResult:
        if not url:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)

        try:
            sqlalchemy = _lazy_import("sqlalchemy", "sqlalchemy")
            engine = sqlalchemy.create_engine(_strip_db_prefix(url))
            try:
                with engine.connect() as connection:
                    connection.execute(sqlalchemy.text("SELECT 1"))
            finally:
                engine.dispose()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def django_database_check(name: str, required: bool = True) -> HealthCheck:
    """Create a Django default database connection check."""

    def check() -> CheckResult:
        try:
            django_db = _lazy_import("django.db", "django")
            with django_db.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def django_cache_check(name: str, cache_alias: str = "default", required: bool = True) -> HealthCheck:
    """Create a Django cache connectivity check."""

    def check() -> CheckResult:
        try:
            django_cache = _lazy_import("django.core.cache", "django")
            cache = django_cache.caches[cache_alias]
            cache.get("__celery_uptime_probe__")
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def mongodb_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create a MongoDB ping health check."""

    def check() -> CheckResult:
        if not url:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)

        try:
            pymongo = _lazy_import("pymongo", "mongodb")
            client = pymongo.MongoClient(url, serverSelectionTimeoutMS=3000)
            try:
                client.admin.command("ping")
            finally:
                client.close()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def elasticsearch_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create an Elasticsearch ping health check."""

    def check() -> CheckResult:
        if not url:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)

        try:
            elasticsearch = _lazy_import("elasticsearch", "elasticsearch")
            client = elasticsearch.Elasticsearch(_normalize_elasticsearch_url(url))
            try:
                client.ping()
            finally:
                close = getattr(client, "close", None)
                if close:
                    close()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def cassandra_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create a Cassandra connection check."""

    def check() -> CheckResult:
        hosts = _cassandra_hosts(url)
        if not hosts:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)

        try:
            cluster_module = _lazy_import("cassandra.cluster", "cassandra")
            cluster = cluster_module.Cluster(hosts)
            session = cluster.connect()
            try:
                session.execute("SELECT now() FROM system.local")
            finally:
                cluster.shutdown()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def memcache_check(name: str, url: str | None, required: bool = True) -> HealthCheck:
    """Create a Memcached version check."""

    def check() -> CheckResult:
        servers = _memcache_servers(url)
        if not servers:
            return CheckResult(name=name, ok=not required, detail="missing_config:url", required=required)

        try:
            memcache = _lazy_import("memcache", "memcache")
            client = memcache.Client(servers)
            try:
                versions = client.get_stats("version")
                if versions is None:
                    return CheckResult(name=name, ok=False, detail="empty_version_response", required=required)
            finally:
                disconnect = getattr(client, "disconnect_all", None)
                if disconnect:
                    disconnect()
        except MissingExtraError as exc:
            return CheckResult(name=name, ok=not required, detail=str(exc), required=required)
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return _named_check(name, check)


def disabled_backend_check(name: str = "backend") -> HealthCheck:
    """Create a successful check for a deliberately disabled result backend."""

    def check() -> CheckResult:
        return CheckResult(name=name, ok=True, detail="disabled", required=False)

    return _named_check(name, check)


def unsupported_check(name: str, detail: str, required: bool = True) -> HealthCheck:
    """Create a check that fails closed for unsupported automatic detection."""

    def check() -> CheckResult:
        return CheckResult(name=name, ok=not required, detail=detail, required=required)

    return _named_check(name, check)


def check_payload(result: CheckResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": result.status,
        "detail": result.detail,
    }
    if not result.required:
        payload["required"] = False
    return payload


class MissingExtraError(ImportError):
    def __init__(self, extra: str) -> None:
        self.extra = extra
        super().__init__(f"missing_extra:{extra}")


def _lazy_import(module: str, extra: str) -> Any:
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as exc:
        if exc.name and (exc.name == module or module.startswith(f"{exc.name}.")):
            raise MissingExtraError(extra) from exc
        raise


def _sentinel_hosts(urls: str | list[str] | tuple[str, ...]) -> list[tuple[str, int]]:
    values = [urls] if isinstance(urls, str) else list(urls)
    hosts: list[tuple[str, int]] = []
    for value in values:
        for raw_url in str(value).split(";"):
            parsed = urlparse(raw_url)
            if parsed.hostname:
                hosts.append((parsed.hostname, parsed.port or 26379))
    return hosts


def _strip_db_prefix(url: str) -> str:
    if url.startswith("db+"):
        return url.removeprefix("db+")
    if url.startswith("database+"):
        return url.removeprefix("database+")
    return url


def _normalize_elasticsearch_url(url: str) -> str:
    if url.startswith("elasticsearch://"):
        return "http://" + url.removeprefix("elasticsearch://")
    return url


def _cassandra_hosts(url: str | None) -> list[str]:
    if not url:
        return []
    parsed = urlparse(url)
    if parsed.hostname:
        return [parsed.hostname]
    return [host.strip() for host in url.split(",") if host.strip()]


def _memcache_servers(url: str | None) -> list[str]:
    if not url:
        return []
    cleaned = url
    for prefix in ("cache+memcached://", "cache+pymemcache://", "cache+pylibmc://", "memcache://"):
        cleaned = cleaned.removeprefix(prefix)
    cleaned = cleaned.strip("/")
    return [server.strip() for server in cleaned.split(";") if server.strip()]


def _extra_from_module(module: str | None) -> str:
    if not module:
        return "broker"
    if module.startswith("confluent_kafka"):
        return "kafka"
    if module.startswith("redis"):
        return "redis"
    if module.startswith("boto"):
        return "sqs"
    return module.split(".", 1)[0].replace("_", "-")


def _named_check(name: str, check: HealthCheck) -> HealthCheck:
    check.__celery_uptime_name__ = name  # type: ignore[attr-defined]
    return check

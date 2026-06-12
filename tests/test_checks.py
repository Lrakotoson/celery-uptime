from botocore.exceptions import ClientError

from celery_uptime.checks import redis_check, sqs_check


def test_redis_check_reports_missing_url():
    result = redis_check("backend", None)()

    assert result.name == "backend"
    assert result.ok is False
    assert result.detail == "missing_config:url"


def test_redis_check_maps_client_exceptions(monkeypatch):
    class Client:
        def ping(self):
            raise ConnectionError("redis down")

        def close(self):
            pass

    monkeypatch.setattr("celery_uptime.checks.redis.from_url", lambda *_args, **_kwargs: Client())

    result = redis_check("backend", "redis://localhost:6379/0")()

    assert result.ok is False
    assert result.detail == "redis down"


def test_sqs_check_reports_missing_config():
    result = sqs_check("broker", None, "fr-par", None, "access", "secret")()

    assert result.name == "broker"
    assert result.ok is False
    assert result.detail == "missing_config:endpoint_url,queue_url"


def test_sqs_check_maps_client_exceptions(monkeypatch):
    class Client:
        def get_queue_attributes(self, **_kwargs):
            raise ClientError(
                {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "missing"}},
                "GetQueueAttributes",
            )

    monkeypatch.setattr("celery_uptime.checks.boto3.client", lambda *_args, **_kwargs: Client())

    result = sqs_check(
        "broker",
        endpoint_url="https://sqs.example",
        region="fr-par",
        queue_url="https://sqs.example/queue",
        access_key="access",
        secret_key="secret",
    )()

    assert result.ok is False
    assert "NonExistentQueue" in result.detail

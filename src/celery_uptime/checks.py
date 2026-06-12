from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import boto3
import redis
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError


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
            client = redis.from_url(url, decode_responses=True)
            try:
                client.ping()
            finally:
                client.close()
        except Exception as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return check


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
            client = boto3.client(
                "sqs",
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=BotoConfig(
                    connect_timeout=2,
                    read_timeout=3,
                    retries={"max_attempts": 1},
                ),
            )
            client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn"],
            )
        except (BotoCoreError, ClientError) as exc:
            return CheckResult(name=name, ok=False, detail=str(exc), required=required)

        return CheckResult(name=name, ok=True, detail="ok", required=required)

    return check


def unsupported_check(name: str, detail: str, required: bool = True) -> HealthCheck:
    """Create a check that fails closed for unsupported automatic detection."""

    def check() -> CheckResult:
        return CheckResult(name=name, ok=not required, detail=detail, required=required)

    return check


def check_payload(result: CheckResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": result.status,
        "detail": result.detail,
    }
    if not result.required:
        payload["required"] = False
    return payload

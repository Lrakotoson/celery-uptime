# celery-uptime

`celery-uptime` adds lightweight HTTP health and readiness endpoints to Celery
workers and beat without changing the Celery command you already run.

```python
from celery import Celery
from celery_uptime import monitor

app = Celery("my-service", broker="sqs://", backend="redis://redis:6379/0")

monitor(app)
```

Then keep your normal command:

```bash
celery -A my_service.celery_app worker --loglevel=info
```

The package starts an embedded Uvicorn server from Celery lifecycle signals. It
starts on `worker_ready` for workers and `beat_init` for beat.

## Endpoints

- `GET /health`: returns process/server liveness.
- `GET /ready`: returns Celery process readiness plus broker/backend checks.

Example `/ready` response:

```json
{
  "status": "ok",
  "service": "my-service-celery-worker",
  "process": "worker",
  "checks": {
    "celery_process": {"status": "ok", "detail": "ready"},
    "broker": {"status": "ok", "detail": "ok"},
    "backend": {"status": "ok", "detail": "ok"}
  }
}
```

## Configuration

Environment variables:

- `CELERY_UPTIME_ENABLED=true`
- `CELERY_UPTIME_HOST=0.0.0.0`
- `CELERY_UPTIME_PORT=8090`
- `CELERY_UPTIME_SERVICE=<celery app main>-celery-<worker|beat>`
- `CELERY_UPTIME_LOG_LEVEL=warning`

Docker Compose example:

```yaml
services:
  celery-worker:
    command: ["celery", "-A", "my_service.celery_app", "worker", "--loglevel=info"]
    environment:
      - CELERY_UPTIME_PORT=8090
    ports:
      - "49211:8090"
    healthcheck:
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/ready', timeout=5).read()\" || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```

## Automatic Checks

`monitor(app)` automatically detects:

- Redis broker from `broker_url=redis://...`
- Redis result backend from `result_backend=redis://...`
- SQS broker from `broker_url=sqs://` and `broker_transport_options`

Unsupported or missing broker/backend configuration fails closed in `/ready`
with HTTP `503`.

## Explicit Checks

For unusual apps, pass explicit checks:

```python
from celery_uptime import monitor, redis_check, sqs_check

monitor(
    app,
    checks=[
        sqs_check(
            "broker",
            endpoint_url="https://sqs.fr-par.scw.cloud",
            region="fr-par",
            queue_url="https://sqs.fr-par.scw.cloud/queue",
            access_key="...",
            secret_key="...",
        ),
        redis_check("backend", "redis://redis:6379/0"),
    ],
    include_auto_checks=False,
)
```

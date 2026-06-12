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

## Installation

The base package only installs Celery, FastAPI, and Uvicorn. Install the extras
matching the providers your Celery app already uses:

```bash
pip install "celery-uptime[redis,sqs]"
```

Available extras:

- `redis`: Redis broker/backend and Redis Sentinel.
- `sqs`: SQS broker.
- `kafka`: Kafka broker through Kombu's Confluent Kafka transport.
- `sqlalchemy`: SQLAlchemy/database result backend.
- `django`: Django database/cache result backends.
- `mongodb`: MongoDB result backend.
- `elasticsearch`: Elasticsearch result backend.
- `cassandra`: Cassandra result backend.
- `memcache`: Memcached cache result backend.
- `all`: all optional provider dependencies.

Missing provider dependencies do not crash the monitor. `/ready` returns a
failing check with details such as `missing_extra:mongodb`.

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

- RabbitMQ/AMQP broker: `amqp://`, `pyamqp://`, `librabbitmq://`.
- Redis broker/backend: `redis://`, `rediss://`.
- Redis Sentinel broker/backend: `sentinel://` with `master_name`.
- SQS broker: `sqs://` with `broker_transport_options`.
- Kafka broker: `kafka://`, `confluentkafka://`.
- SQLAlchemy/database backend: `db+...`, `database+...`.
- Django backend conventions: `django-db`, `django-cache`.
- RPC backend: `rpc://`, checked through broker connectivity.
- Memcached backend: `cache+memcached://`, `cache+pymemcache://`, `cache+pylibmc://`.
- MongoDB backend: `mongodb://`, `mongodb+srv://`.
- Elasticsearch backend: `elasticsearch://`.
- Cassandra backend: `cassandra://`.

No result backend is valid Celery configuration. If `result_backend` is absent
or `disabled://`, `/ready` reports the backend as healthy and non-required:

```json
{"status": "ok", "detail": "disabled", "required": false}
```

Unsupported providers outside this classic set fail closed in `/ready` unless
you replace them with explicit checks.

## Explicit Checks

For unusual apps, pass explicit checks:

```python
from celery_uptime import database_check, monitor, redis_check, sqs_check

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
        database_check("reporting-db", "postgresql://user:password@db:5432/app"),
    ],
    include_auto_checks=False,
)
```

Provider checks are connection-only. They do not write/read/delete Celery result
records or mutate broker/backend data.

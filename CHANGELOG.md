# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Isolated the embedded Uvicorn asyncio loop from Celery's gevent hub by running it in a native OS thread.
- Disabled Uvicorn signal handling in the background server thread and bounded shutdown waiting.
- Made repeated `monitor(app)` registration idempotent and marked readiness false during shutdown.

## [0.0.1] - 2026-06-16

### Added

- Embedded Uvicorn health server started from Celery worker and beat lifecycle signals.
- `/health` liveness endpoint for Docker/container healthchecks.
- `/ready` cached dependency readiness endpoint for external monitoring.
- Automatic checks for classic Celery brokers and result backends.
- Optional provider extras for Redis, SQS, Kafka, SQLAlchemy, Django, MongoDB, Elasticsearch, Cassandra, and Memcached.
- Explicit check helpers for unusual broker/backend configurations.
- Release v0.0.1.

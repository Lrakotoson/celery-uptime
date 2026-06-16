# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1] - 2026-06-16

### Changed

- Release v0.0.1.

## [0.1.0] - 2026-06-16

### Added

- Embedded Uvicorn health server started from Celery worker and beat lifecycle signals.
- `/health` liveness endpoint for Docker/container healthchecks.
- `/ready` cached dependency readiness endpoint for external monitoring.
- Automatic checks for classic Celery brokers and result backends.
- Optional provider extras for Redis, SQS, Kafka, SQLAlchemy, Django, MongoDB, Elasticsearch, Cassandra, and Memcached.
- Explicit check helpers for unusual broker/backend configurations.

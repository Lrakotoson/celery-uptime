# Contributing

## Development Setup

```bash
uv sync --all-extras --dev
uv run pytest
uv run ruff check .
uv build
```

The package uses a `src/` layout and exposes type information through
`py.typed`.

## Design Rules

- Keep `monitor(app)` as the smooth default integration path.
- Keep provider dependencies optional and imported lazily.
- Do not run broker/backend probes in `/health`.
- Do not make worker saturation or full capacity an unhealthy state.
- Keep provider checks connection-only unless a future major version documents
  a mutating readiness mode explicitly.

## Release Checklist

1. Update `version` in `pyproject.toml`.
2. Update `CHANGELOG.md`.
3. Run `uv run pytest`.
4. Run `uv run ruff check .`.
5. Run `uv build`.
6. Run `uv run twine check dist/*`.
7. Publish only from a clean git tree.

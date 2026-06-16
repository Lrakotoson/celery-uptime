# Publishing

`celery-uptime` publishes from GitHub Releases. The release tag must match the
version stored in `pyproject.toml`, so the project uses a separate version-bump
workflow before publishing.

## Release flow

1. Go to **Actions** > **Bump version**.
2. Run the workflow from the `main` branch with a PEP 440 version such as
   `0.2.0`. Do not include the leading `v`.
3. The workflow updates `pyproject.toml` and `CHANGELOG.md`, commits
   `chore(release): vX.Y.Z`, and creates the tag `vX.Y.Z`.
4. Create and publish a GitHub Release from that tag.
5. The **Publish package** workflow validates the tag, runs tests and package
   checks, builds the distributions, and uploads them to PyPI.

The publish workflow intentionally does not bump the version. Published package
artifacts should be built from the exact source referenced by the release tag.

## Local checks

Run the same checks before cutting a release:

```bash
uv sync --dev
uv run pytest
uv run ruff check .
python3 -m py_compile src/celery_uptime/*.py
uv build
uv run twine check dist/*
```

## Manual upload fallback

Prefer the GitHub Release workflow. For an emergency manual upload, build and
check the package locally, then publish with Twine using environment variables:

```bash
uv build
uv run twine check dist/*
TWINE_USERNAME=__token__ TWINE_PASSWORD="$PYPI_TOKEN" uv run twine upload dist/*
```

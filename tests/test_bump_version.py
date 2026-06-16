from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bump_version.py"
SPEC = importlib.util.spec_from_file_location("bump_version", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
bump_version = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bump_version
SPEC.loader.exec_module(bump_version)

ReleaseMetadata = bump_version.ReleaseMetadata
bump_changelog = bump_version.bump_changelog
bump_pyproject_version = bump_version.bump_pyproject_version
validate_version = bump_version.validate_version


def test_validate_version_rejects_leading_v() -> None:
    with pytest.raises(ValueError, match="leading 'v'"):
        validate_version("v0.2.0")


def test_validate_version_rejects_surrounding_whitespace() -> None:
    with pytest.raises(ValueError, match="surrounding whitespace"):
        validate_version(" 0.2.0")


def test_validate_version_accepts_release_version() -> None:
    assert validate_version("0.2.0") == "0.2.0"


def test_bump_pyproject_version_updates_project_static_version(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "celery-uptime"\nversion = "0.1.0"\n\n[tool.pytest.ini_options]\n',
        encoding="utf-8",
    )

    bump_pyproject_version(pyproject, "0.2.0")

    assert 'version = "0.2.0"' in pyproject.read_text(encoding="utf-8")


def test_bump_changelog_replaces_unreleased_heading(tmp_path: Path) -> None:
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(
        "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- New release notes.\n\n## [0.1.0] - 2026-06-16\n",
        encoding="utf-8",
    )

    bump_changelog(changelog, ReleaseMetadata(version="0.2.0", release_date=date(2026, 6, 16)))

    content = changelog.read_text(encoding="utf-8")
    assert "## [0.2.0] - 2026-06-16" in content
    assert "## [Unreleased]" not in content
    assert "- New release notes." in content


def test_bump_changelog_inserts_release_when_unreleased_is_missing(tmp_path: Path) -> None:
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text("# Changelog\n\nIntro.\n\n## [0.1.0] - 2026-06-16\n", encoding="utf-8")

    bump_changelog(changelog, ReleaseMetadata(version="0.2.0", release_date=date(2026, 6, 16)))

    content = changelog.read_text(encoding="utf-8")
    assert content.index("## [0.2.0] - 2026-06-16") < content.index("## [0.1.0] - 2026-06-16")
    assert "- Release v0.2.0." in content

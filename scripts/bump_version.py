"""Bump celery-uptime release metadata.

This script is intentionally dependency-free so GitHub Actions can run it before
installing the package environment.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

VERSION_RE = re.compile(
    r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:(?:a|b|rc)(?:0|[1-9]\d*))?"
    r"(?:\.post(?:0|[1-9]\d*))?"
    r"(?:\.dev(?:0|[1-9]\d*))?$"
)


@dataclass(frozen=True)
class ReleaseMetadata:
    version: str
    release_date: date

    @property
    def tag(self) -> str:
        return f"v{self.version}"


def validate_version(version: str) -> str:
    normalized = version.strip()
    if normalized != version:
        raise ValueError("version must not contain surrounding whitespace")
    if normalized.startswith("v"):
        raise ValueError("version must not include a leading 'v'")
    if not VERSION_RE.fullmatch(normalized):
        raise ValueError("version must be a PEP 440 release like '0.2.0', '0.2.0rc1', or '0.2.0.post1'")
    return normalized


def bump_pyproject_version(pyproject_path: Path, version: str) -> None:
    content = pyproject_path.read_text(encoding="utf-8")
    project_section = re.search(r"(?ms)^\[project\]\n(?P<body>.*?)(?=^\[|\Z)", content)
    if project_section is None:
        raise ValueError("pyproject.toml is missing a [project] section")

    section = project_section.group(0)
    updated_section, replacements = re.subn(
        r'(?m)^version = "[^"]+"$',
        f'version = "{version}"',
        section,
        count=1,
    )
    if replacements != 1:
        raise ValueError("pyproject.toml [project] section is missing a static version")

    updated = content[: project_section.start()] + updated_section + content[project_section.end() :]
    pyproject_path.write_text(updated, encoding="utf-8")


def bump_changelog(changelog_path: Path, metadata: ReleaseMetadata) -> None:
    content = changelog_path.read_text(encoding="utf-8")
    release_heading = f"## [{metadata.version}] - {metadata.release_date.isoformat()}"

    if re.search(rf"(?m)^## \[{re.escape(metadata.version)}\](?: - .*)?$", content):
        raise ValueError(f"CHANGELOG.md already contains a section for {metadata.version}")

    unreleased_pattern = re.compile(r"(?m)^## \[Unreleased\](?:\n|$)")
    if unreleased_pattern.search(content):
        updated = unreleased_pattern.sub(f"{release_heading}\n", content, count=1)
        changelog_path.write_text(updated, encoding="utf-8")
        return

    first_release_heading = re.search(r"(?m)^## \[", content)
    if first_release_heading is None:
        raise ValueError("CHANGELOG.md does not contain a release section")

    section = f"{release_heading}\n\n### Changed\n\n- Release {metadata.tag}.\n\n"
    updated = content[: first_release_heading.start()] + section + content[first_release_heading.start() :]
    changelog_path.write_text(updated, encoding="utf-8")


def bump_release(repo_root: Path, metadata: ReleaseMetadata) -> None:
    bump_pyproject_version(repo_root / "pyproject.toml", metadata.version)
    bump_changelog(repo_root / "CHANGELOG.md", metadata)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bump celery-uptime release metadata.")
    parser.add_argument("version", help="PEP 440 version without a leading 'v', for example 0.2.0.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd(), help="Repository root. Defaults to cwd.")
    parser.add_argument(
        "--date",
        dest="release_date",
        default=date.today().isoformat(),
        help="Release date in YYYY-MM-DD format. Defaults to today.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    version = validate_version(args.version)
    release_date = date.fromisoformat(args.release_date)
    bump_release(args.repo_root.resolve(), ReleaseMetadata(version=version, release_date=release_date))


if __name__ == "__main__":
    main()

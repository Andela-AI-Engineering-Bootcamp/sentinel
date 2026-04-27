"""Create Lambda zip packages for Sentinel services and agents.

This script does not require Docker for the current pure-Python MVP.
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMMON_DIR = ROOT / "common"
INTEGRATIONS_DIR = ROOT / "integrations"
TEMP_PKG_DIR = ROOT / "temp_pkg"
AGENTS = ["planner", "normalizer", "summarizer", "investigator", "remediator"]


def _packagable_py(path: Path) -> bool:
    """Omit pytest modules and other *_*_test.py helpers from Lambda zips."""
    name = path.name
    stem = path.stem
    if name.startswith("test_") or stem.endswith("_test"):
        return False
    return True


def _write_common(zf: zipfile.ZipFile) -> None:
    for path in COMMON_DIR.rglob("*.py"):
        if _packagable_py(path):
            zf.write(path, path.relative_to(ROOT).as_posix())


def _write_integrations(zf: zipfile.ZipFile) -> None:
    if not INTEGRATIONS_DIR.is_dir():
        return
    for path in INTEGRATIONS_DIR.rglob("*.py"):
        if _packagable_py(path):
            zf.write(path, path.relative_to(ROOT).as_posix())


def _write_extra_dir(zf: zipfile.ZipFile, dir_name: str) -> None:
    target_dir = ROOT / dir_name
    if not target_dir.is_dir():
        return
    for path in target_dir.rglob("*.py"):
        if _packagable_py(path):
            zf.write(path, path.relative_to(ROOT).as_posix())


def _write_dependencies(zf: zipfile.ZipFile) -> None:
    if not TEMP_PKG_DIR.is_dir():
        return
    for path in TEMP_PKG_DIR.rglob("*"):
        if path.is_file():
            zf.write(path, path.relative_to(TEMP_PKG_DIR).as_posix())


def _build_agent(agent: str) -> Path:
    agent_dir = ROOT / agent
    out = agent_dir / f"{agent}_lambda.zip"
    if out.exists():
        out.unlink()

    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_name in ["lambda_handler.py", "agent.py", "templates.py", "__init__.py"]:
            path = agent_dir / file_name
            if path.exists():
                zf.write(path, file_name)
        _write_common(zf)
        _write_integrations(zf)
        _write_dependencies(zf)

    return out


def _build_dir_zip(source_dir: Path, zip_name: str, files: list[str]) -> Path:
    out = source_dir / zip_name
    if out.exists():
        out.unlink()

    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_name in files:
            path = source_dir / file_name
            if path.exists():
                zf.write(path, f"{source_dir.name}/{file_name}")
        _write_common(zf)
        _write_integrations(zf)
        if source_dir.name == "api":
            _write_extra_dir(zf, "investigator")
            _write_extra_dir(zf, "comparator")
            _write_extra_dir(zf, "replay")
        _write_dependencies(zf)

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-api", action="store_true")
    parser.add_argument("--include-ingest", action="store_true")
    args = parser.parse_args()

    print("Packaging Sentinel agents...")
    for agent in AGENTS:
        artifact = _build_agent(agent)
        print(f"- {agent}: {artifact}")

    if args.include_api:
        artifact = _build_dir_zip(ROOT / "api", "api_lambda.zip", ["lambda_handler.py", "main.py", "auth.py", "__init__.py"])
        print(f"- api: {artifact}")


    if args.include_ingest:
        artifact = _build_dir_zip(ROOT / "ingest", "ingest_lambda.zip", ["ingest_lambda.py", "__init__.py"])
        print(f"- ingest: {artifact}")

    print("Packaging complete.")


if __name__ == "__main__":
    main()

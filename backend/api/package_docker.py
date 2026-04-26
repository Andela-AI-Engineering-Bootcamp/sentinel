"""Build a Lambda deployment package with Linux-compatible dependencies."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import tomllib
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
BACKEND_ROOT = ROOT.parent
ZIP_PATH = ROOT / "api_lambda.zip"
DOCKER_IMAGE = "public.ecr.aws/lambda/python:3.12"
DOCKER_PLATFORM = "linux/amd64"
SKIP_DIRS = {"__pycache__", ".venv"}
SKIP_SUFFIXES = {".pyc", ".pyo"}


def _copy_ignore(dirpath: str, names: list[str]) -> list[str]:
    """Skip caches, bytecode, and pytest-only modules (same rules as package_docker.py)."""
    ignored: list[str] = []
    base = Path(dirpath)
    for name in names:
        if name in SKIP_DIRS:
            ignored.append(name)
            continue
        if name.endswith((".pyc", ".pyo")):
            ignored.append(name)
            continue
        path = base / name
        if path.is_file():
            if path.suffix != ".py":
                ignored.append(name)
                continue
            stem = path.stem
            if name.startswith("test_") or stem.endswith("_test"):
                ignored.append(name)
    return ignored


def _source_dirs() -> list[str]:
    return sorted(
        path.name
        for path in BACKEND_ROOT.iterdir()
        if path.is_dir()
        and path.name not in SKIP_DIRS
        and (path / "__init__.py").exists()
    )


def _dependencies() -> list[str]:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return list(pyproject["project"]["dependencies"])


def _docker_build(target_dir: Path) -> None:
    deps = _dependencies()
    install_cmd = [
        "docker",
        "run",
        "--rm",
        "--platform",
        DOCKER_PLATFORM,
        "--entrypoint",
        "python",
        "-v",
        f"{target_dir}:/out",
        DOCKER_IMAGE,
        "-m",
        "pip",
        "install",
        "--target",
        "/out",
        *deps,
    ]
    subprocess.run(install_cmd, check=True)


def _copy_sources(target_dir: Path) -> None:
    for source_name in _source_dirs():
        src = BACKEND_ROOT / source_name
        dst = target_dir / source_name
        shutil.copytree(
            src,
            dst,
            dirs_exist_ok=True,
            ignore=_copy_ignore,
        )


def _zip_tree(source_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in source_dir.rglob("*"):
            rel_parts = path.relative_to(source_dir).parts
            if any(part in SKIP_DIRS for part in rel_parts):
                continue
            if path.is_file() and path.suffix not in SKIP_SUFFIXES:
                zf.write(path, path.relative_to(source_dir).as_posix())


def main() -> None:
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        build_dir = Path(tmpdir) / "lambda-build"
        build_dir.mkdir(parents=True, exist_ok=True)

        _docker_build(build_dir)
        _copy_sources(build_dir)
        _zip_tree(build_dir, ZIP_PATH)

    size_mb = ZIP_PATH.stat().st_size / (1024 * 1024)
    print(f"Created {ZIP_PATH.name} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()

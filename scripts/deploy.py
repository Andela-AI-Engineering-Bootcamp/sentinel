"""Deploy helper for frontend static assets and optional CloudFront invalidation."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
TERRAFORM_FRONTEND = ROOT / "terraform" / "7_frontend"


def load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def run(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True, env=env)


def capture(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    proc = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True, env=env)
    return proc.stdout.strip()


def resolve_api_url(env: dict[str, str]) -> str:
    explicit = env.get("NEXT_PUBLIC_API_URL", "").strip()
    if explicit:
      return explicit.rstrip("/")

    try:
        api_url = capture(["terraform", "output", "-raw", "api_gateway_url"], cwd=TERRAFORM_FRONTEND, env=env)
    except Exception:
        api_url = ""

    return api_url.rstrip("/")


def is_local_url(url: str) -> bool:
    lowered = url.lower()
    return lowered.startswith("http://localhost") or lowered.startswith("https://localhost") or "127.0.0.1" in lowered


def clear_next_cache() -> None:
    cache_dir = FRONTEND / ".next" / "cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def main() -> None:
    env = os.environ.copy()
    env.update({k: v for k, v in load_dotenv(ROOT / ".env").items() if k not in env})

    api_url = resolve_api_url(env)
    if not api_url:
        raise SystemExit(
            "Could not determine NEXT_PUBLIC_API_URL. "
            "Set it in your shell/.env or ensure terraform/7_frontend has a valid api_gateway_url output."
        )

    bucket = env.get("SENTINEL_FRONTEND_BUCKET", "")
    if bucket and is_local_url(api_url):
        raise SystemExit(
            f"Refusing to deploy frontend with NEXT_PUBLIC_API_URL={api_url}. "
            "Use a real API Gateway URL for non-local builds."
        )

    env["NEXT_PUBLIC_API_URL"] = api_url
    print(f"Building frontend with NEXT_PUBLIC_API_URL={api_url}")
    clear_next_cache()
    run(["npm", "run", "build"], cwd=FRONTEND, env=env)

    dist_id = env.get("SENTINEL_CLOUDFRONT_DISTRIBUTION_ID", "")
    if not bucket:
        print("Build complete. Set SENTINEL_FRONTEND_BUCKET to enable S3 upload.")
        return

    run(["aws", "s3", "sync", str(FRONTEND / "out"), f"s3://{bucket}", "--delete"], env=env)
    print(f"Uploaded frontend to s3://{bucket}")

    if dist_id:
        run(["aws", "cloudfront", "create-invalidation", "--distribution-id", dist_id, "--paths", "/*"], env=env)
        print(f"Invalidated CloudFront distribution {dist_id}")


if __name__ == "__main__":
    main()

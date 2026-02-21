#!/usr/bin/env python3
"""Upload local notebook source files to Databricks workspace."""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

DBX_NOTEBOOK_BASE = "/Shared/data-5035/week07/clinical_trial_demo"
NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "databricks" / "notebooks"


def _request(method: str, url: str, token: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url=url, method=method, data=body)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


def main() -> int:
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host or not token:
        print("Missing required environment variables.")
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, then rerun.")
        return 1

    if not NOTEBOOK_DIR.exists():
        print(f"Notebook directory does not exist: {NOTEBOOK_DIR}")
        return 1

    mkdir_url = f"{host}/api/2.0/workspace/mkdirs"
    import_url = f"{host}/api/2.0/workspace/import"

    try:
        _request("POST", mkdir_url, token, {"path": DBX_NOTEBOOK_BASE})
    except urllib.error.HTTPError as exc:
        print(f"Failed to create workspace directory {DBX_NOTEBOOK_BASE}: {exc}")
        return 1

    uploaded = 0
    for path in sorted(NOTEBOOK_DIR.glob("*.py")):
        content = base64.b64encode(path.read_bytes()).decode("utf-8")
        target = f"{DBX_NOTEBOOK_BASE}/{path.stem}"
        payload = {
            "path": target,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": content,
        }
        try:
            _request("POST", import_url, token, payload)
            uploaded += 1
            print(f"Uploaded {path.name} -> {target}")
        except urllib.error.HTTPError as exc:
            print(f"Failed to upload {path.name}: {exc}")
            return 1

    print(f"Upload complete. Uploaded {uploaded} notebook(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

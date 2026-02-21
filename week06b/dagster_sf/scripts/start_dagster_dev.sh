#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "Tip: activate your data5035 environment first (pyenv activate data5035)."
fi

cd "$ROOT_DIR"
exec dagster dev -m dagster_sf.definitions -p 3000

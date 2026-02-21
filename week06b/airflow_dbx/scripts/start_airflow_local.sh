#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_HOME_DEFAULT="$ROOT_DIR/.airflow"
DAGS_FOLDER_DEFAULT="$ROOT_DIR/dags"

export AIRFLOW_HOME="${AIRFLOW_HOME:-$AIRFLOW_HOME_DEFAULT}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-$DAGS_FOLDER_DEFAULT}"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-admin:admin}"

AIRFLOW_BIN_DEFAULT="$HOME/.pyenv/versions/data5035/bin/airflow"
if [[ -x "$AIRFLOW_BIN_DEFAULT" ]]; then
  AIRFLOW_BIN="$AIRFLOW_BIN_DEFAULT"
else
  AIRFLOW_BIN="$(command -v airflow || true)"
fi

if [[ -z "$AIRFLOW_BIN" ]]; then
  echo "Could not find airflow binary. Activate your data5035 environment first."
  exit 1
fi

usage() {
  cat <<EOF
Usage: $(basename "$0") <init|reserialize|list-local|api-server|scheduler|env>

Commands:
  init         Reset and migrate metadata DB in AIRFLOW_HOME
  reserialize  Parse and serialize DAGs into metadata DB
  list-local   Show DAGs parsed directly from AIRFLOW__CORE__DAGS_FOLDER
  api-server   Start Airflow API/UI server on port 8080
  scheduler    Start Airflow scheduler
  env          Print effective Airflow environment settings
EOF
}

cmd="${1:-}"
case "$cmd" in
  init)
    "$AIRFLOW_BIN" db reset -y
    "$AIRFLOW_BIN" db migrate
    ;;
  reserialize)
    "$AIRFLOW_BIN" dags reserialize
    ;;
  list-local)
    "$AIRFLOW_BIN" dags list --local
    ;;
  api-server)
    "$AIRFLOW_BIN" api-server --port 8080
    ;;
  scheduler)
    "$AIRFLOW_BIN" scheduler
    ;;
  env)
    echo "AIRFLOW_BIN=$AIRFLOW_BIN"
    echo "AIRFLOW_HOME=$AIRFLOW_HOME"
    echo "AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"
    echo "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"
    ;;
  *)
    usage
    exit 1
    ;;
esac

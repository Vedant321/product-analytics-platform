#!/usr/bin/env bash
set -euo pipefail

dashboard_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "$dashboard_dir/.." && pwd)"
web_dir="$dashboard_dir/web"
web_port="${DASHBOARD_WEB_PORT:-5173}"
venv_dir="$dashboard_dir/.venv"
python_bin="$venv_dir/bin/python"
backend_pid=""

cleanup() {
  if [[ -n "$backend_pid" ]]; then
    kill "$backend_pid" 2>/dev/null || true
    wait "$backend_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install it from https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi
if [[ ! -x "$python_bin" ]]; then
  echo "Creating Python 3.12 environment..."
  uv venv --python 3.12 "$venv_dir"
fi
if ! "$python_bin" -c "import fastapi, uvicorn, databricks.sdk, numpy, pandas, statsmodels" >/dev/null 2>&1; then
  echo "Installing Python API dependencies..."
  uv pip install --python "$python_bin" -r "$dashboard_dir/backend/requirements.txt"
fi
if [[ ! -d "$web_dir/node_modules" ]]; then
  echo "Installing React dependencies..."
  npm install --prefix "$web_dir"
fi

cd "$repo_dir"
requested_mode="${DEMO_MODE:-false}"
health_response="$(curl --fail --silent --max-time 1 http://127.0.0.1:8000/api/health || true)"
if [[ -n "$health_response" ]]; then
  if [[ "$requested_mode" == "true" && "$health_response" != *'"mode":"demo"'* ]] || [[ "$requested_mode" != "true" && "$health_response" != *'"mode":"live"'* ]]; then
    echo "Port 8000 already runs the dashboard in a different data mode. Stop that server before restarting." >&2
    exit 1
  fi
  echo "Using the existing dashboard API on port 8000."
else
  if [[ "$requested_mode" != "true" ]]; then
    echo "Checking Databricks authentication..."
    if ! "$python_bin" -m dashboard.backend.check_connection; then
      echo "Live mode requires a valid Databricks profile. See dashboard/README.md." >&2
      exit 1
    fi
  fi
  echo "Starting dashboard API in ${requested_mode} mode..."
  DEMO_MODE="$requested_mode" "$python_bin" -m uvicorn dashboard.backend.main:app --host 127.0.0.1 --port 8000 --reload &
  backend_pid=$!
  ready=0
  for _ in {1..100}; do
    if curl --fail --silent --max-time 1 http://127.0.0.1:8000/api/health >/dev/null; then
      ready=1
      break
    fi
    if ! kill -0 "$backend_pid" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  if [[ "$ready" != 1 ]]; then
    echo "The API did not start. Check the error above and make sure port 8000 is free." >&2
    exit 1
  fi
fi

echo "Dashboard ready at http://localhost:$web_port"
cd "$web_dir"
npm run vite -- --port "$web_port" --strictPort

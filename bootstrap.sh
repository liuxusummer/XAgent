#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEB_DIR="$ROOT_DIR/frontends/web"

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-7861}"
WEB_HOST="${WEB_HOST:-127.0.0.1}"
WEB_PORT="${WEB_PORT:-5173}"
GRADIO_PORT="${GRADIO_PORT:-7860}"

PYTHON_CMD=()

usage() {
  cat <<EOF
Usage: ./bootstrap.sh <command>

Commands:
  install     Install Python web deps and frontend npm deps
  api         Start FastAPI backend on ${API_HOST}:${API_PORT}
  web         Start Vite frontend on ${WEB_HOST}:${WEB_PORT}
  dev         Start FastAPI backend and Vite frontend together
  build-web   Build the React frontend
  serve       Build frontend, then serve it from FastAPI
  gradio      Start Gradio UI on ${API_HOST}:${GRADIO_PORT}
  help        Show this help

Environment:
  API_HOST=${API_HOST}
  API_PORT=${API_PORT}
  WEB_HOST=${WEB_HOST}
  WEB_PORT=${WEB_PORT}
  GRADIO_PORT=${GRADIO_PORT}
EOF
}

detect_python() {
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
    PYTHON_CMD=("$ROOT_DIR/.venv/bin/python")
  elif command -v uv >/dev/null 2>&1; then
    PYTHON_CMD=(uv run python)
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
  else
    echo "No Python runtime found. Install uv or create .venv first." >&2
    exit 1
  fi
}

require_npm() {
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required for frontend commands." >&2
    exit 1
  fi
}

port_pids() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u || true
  else
    echo "lsof is required to detect port usage." >&2
    exit 1
  fi
}

wait_for_port_free() {
  local port="$1"
  local attempts="${2:-20}"
  local delay="${3:-0.2}"

  for _ in $(seq 1 "$attempts"); do
    if [[ -z "$(port_pids "$port")" ]]; then
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

free_port() {
  local port="$1"
  local label="$2"
  local pids
  pids="$(port_pids "$port")"
  if [[ -z "$pids" ]]; then
    return 0
  fi

  echo "Port $port is already in use for $label. Stopping process(es): $pids"
  kill $pids >/dev/null 2>&1 || true
  if wait_for_port_free "$port"; then
    return 0
  fi

  pids="$(port_pids "$port")"
  if [[ -n "$pids" ]]; then
    echo "Port $port is still busy. Force stopping process(es): $pids"
    kill -9 $pids >/dev/null 2>&1 || true
  fi
  if ! wait_for_port_free "$port" 10 0.2; then
    echo "Failed to free port $port for $label." >&2
    exit 1
  fi
}

install_deps() {
  if command -v uv >/dev/null 2>&1; then
    (cd "$ROOT_DIR" && uv sync --extra web)
  else
    echo "uv is not installed; skipping Python dependency install." >&2
  fi
  require_npm
  npm --prefix "$WEB_DIR" install
}

start_api() {
  detect_python
  free_port "$API_PORT" "FastAPI backend"
  (cd "$ROOT_DIR" && "${PYTHON_CMD[@]}" -m src.web_ui_new --host "$API_HOST" --port "$API_PORT")
}

start_web() {
  require_npm
  free_port "$WEB_PORT" "Vite frontend"
  VITE_API_BASE="http://${API_HOST}:${API_PORT}" \
    npm --prefix "$WEB_DIR" run dev -- --host "$WEB_HOST" --port "$WEB_PORT"
}

build_web() {
  require_npm
  npm --prefix "$WEB_DIR" run build
}

serve_built_web() {
  build_web
  start_api
}

start_gradio() {
  detect_python
  free_port "$GRADIO_PORT" "Gradio UI"
  (cd "$ROOT_DIR" && "${PYTHON_CMD[@]}" -m src.web_ui --host "$API_HOST" --port "$GRADIO_PORT")
}

start_dev() {
  detect_python
  require_npm

  local api_pid=""
  local web_pid=""

  cleanup() {
    local exit_code=$?
    trap - INT TERM EXIT
    if [[ -n "$web_pid" ]] && kill -0 "$web_pid" >/dev/null 2>&1; then
      kill "$web_pid" >/dev/null 2>&1 || true
    fi
    if [[ -n "$api_pid" ]] && kill -0 "$api_pid" >/dev/null 2>&1; then
      kill "$api_pid" >/dev/null 2>&1 || true
    fi
    wait "$web_pid" "$api_pid" >/dev/null 2>&1 || true
    exit "$exit_code"
  }

  trap cleanup INT TERM EXIT

  free_port "$API_PORT" "FastAPI backend"
  free_port "$WEB_PORT" "Vite frontend"

  echo "Starting backend: http://${API_HOST}:${API_PORT}"
  (cd "$ROOT_DIR" && "${PYTHON_CMD[@]}" -m src.web_ui_new --host "$API_HOST" --port "$API_PORT") &
  api_pid=$!

  echo "Starting frontend: http://${WEB_HOST}:${WEB_PORT}"
  VITE_API_BASE="http://${API_HOST}:${API_PORT}" \
    npm --prefix "$WEB_DIR" run dev -- --host "$WEB_HOST" --port "$WEB_PORT" &
  web_pid=$!

  wait -n "$api_pid" "$web_pid"
}

command="${1:-help}"
case "$command" in
  install)
    install_deps
    ;;
  api)
    start_api
    ;;
  web)
    start_web
    ;;
  dev)
    start_dev
    ;;
  build-web)
    build_web
    ;;
  serve)
    serve_built_web
    ;;
  gradio)
    start_gradio
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac

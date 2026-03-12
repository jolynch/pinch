#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./bench.sh [options]

Options:
  --flamegraph               Run start benchmark under perf and emit flamegraph SVG.
  --build                    Build ./pinch before benchmarking (default: true).
  --no-build                 Skip build step.
  --server-url ADDR          File listener address for CLI (default: 127.0.0.1:3453).
  --server-startup-timeout S Seconds to wait for file listener readiness (default: 30).
  --source-directory PATH    Source directory for transfer (default: /home/jolynch/Hacking/test-data).
  --manifest PATH            Manifest output path (default: /tmp/pinch/manifest).
  --out-root PATH            Download output root (default: /var/lib/pinch/data).
  --concurrency N            Start command concurrency (default: 128).
  --zerocopy                 Deprecated (ignored by TCP-only client).
  --encrypt MODE            Encryption mode passed to CLI (supported: age).
  --freq HZ                  perf sample frequency (default: 199).
  --perf-data PATH           perf.data output path (default: /tmp/pinch-start.perf.data).
  --flamegraph-svg PATH      Flamegraph SVG output path (default: /tmp/pinch-start.svg).
  --server-perf-data PATH    Server perf.data output path (default: /tmp/pinch-server.perf.data).
  --server-flamegraph-svg PATH
                             Server flamegraph SVG output path (default: /tmp/pinch-server.svg).
  --flamegraph-dir PATH      Path to FlameGraph repo (optional if scripts on PATH).
  -h, --help                 Show help.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_value() {
  local opt="$1"
  if [[ $# -lt 2 || -z "${2:-}" || "${2:-}" == --* ]]; then
    echo "missing value for ${opt}" >&2
    usage >&2
    exit 2
  fi
}

BUILD=true
FLAMEGRAPH=false
SERVER_URL="127.0.0.1:3453"
SERVER_STARTUP_TIMEOUT_SEC=30
SOURCE_DIRECTORY="/home/jolynch/Hacking/test-data"
MANIFEST_PATH="/tmp/pinch/manifest"
OUT_ROOT="/var/lib/pinch/data"
CONCURRENCY="48"
USE_ZEROCOPY=false
ENCRYPT_MODE=""
PERF_FREQ="199"
PERF_DATA="/tmp/pinch-start.perf.data"
FLAMEGRAPH_SVG="/tmp/pinch-start.svg"
SERVER_PERF_DATA="/tmp/pinch-server.perf.data"
SERVER_FLAMEGRAPH_SVG="/tmp/pinch-server.svg"
FLAMEGRAPH_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --flamegraph)
      FLAMEGRAPH=true
      shift
      ;;
    --build)
      BUILD=true
      shift
      ;;
    --no-build)
      BUILD=false
      shift
      ;;
    --server-url)
      require_value "$1" "${2:-}"
      SERVER_URL="$2"
      shift 2
      ;;
    --source-directory)
      require_value "$1" "${2:-}"
      SOURCE_DIRECTORY="$2"
      shift 2
      ;;
    --server-startup-timeout)
      require_value "$1" "${2:-}"
      SERVER_STARTUP_TIMEOUT_SEC="$2"
      shift 2
      ;;
    --manifest)
      require_value "$1" "${2:-}"
      MANIFEST_PATH="$2"
      shift 2
      ;;
    --out-root)
      require_value "$1" "${2:-}"
      OUT_ROOT="$2"
      shift 2
      ;;
    --concurrency)
      require_value "$1" "${2:-}"
      CONCURRENCY="$2"
      shift 2
      ;;
    --zerocopy)
      USE_ZEROCOPY=true
      shift
      ;;
    --encrypt)
      require_value "$1" "${2:-}"
      ENCRYPT_MODE="$2"
      shift 2
      ;;
    --freq)
      require_value "$1" "${2:-}"
      PERF_FREQ="$2"
      shift 2
      ;;
    --perf-data)
      require_value "$1" "${2:-}"
      PERF_DATA="$2"
      shift 2
      ;;
    --flamegraph-svg)
      require_value "$1" "${2:-}"
      FLAMEGRAPH_SVG="$2"
      shift 2
      ;;
    --server-perf-data)
      require_value "$1" "${2:-}"
      SERVER_PERF_DATA="$2"
      shift 2
      ;;
    --server-flamegraph-svg)
      require_value "$1" "${2:-}"
      SERVER_FLAMEGRAPH_SVG="$2"
      shift 2
      ;;
    --flamegraph-dir)
      require_value "$1" "${2:-}"
      FLAMEGRAPH_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

require_cmd go
require_cmd rm
require_cmd time

if [[ -n "${ENCRYPT_MODE}" && "${ENCRYPT_MODE}" != "age" ]]; then
  echo "unsupported --encrypt value: ${ENCRYPT_MODE} (only 'age' is supported)" >&2
  exit 2
fi
if [[ "${USE_ZEROCOPY}" == "true" && -n "${ENCRYPT_MODE}" ]]; then
  echo "--zerocopy cannot be combined with --encrypt" >&2
  exit 2
fi

if [[ "$BUILD" == "true" ]]; then
  echo "Building pinch..."
  go build -o pinch
fi

SERVER_IN="/tmp/pinch/in"
SERVER_OUT="/tmp/pinch/out"
SERVER_KEYS="/tmp/pinch/keys"
SERVER_LOG="/tmp/pinch-bench-server.log"
SERVER_PID=""
SERVER_APP_PID=""
SERVER_PERF_PID=""

stop_server_perf() {
  if [[ -n "${SERVER_PERF_PID}" ]] && kill -0 "${SERVER_PERF_PID}" >/dev/null 2>&1; then
    sudo kill -INT "${SERVER_PERF_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PERF_PID}" >/dev/null 2>&1 || true
  fi
}

cleanup_server() {
  stop_server_perf
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}

split_host_port() {
  local addr="$1"
  local host="${addr%:*}"
  local port="${addr##*:}"
  if [[ -z "${host}" || -z "${port}" || "${host}" == "${addr}" ]]; then
    echo "invalid --server-url address: ${addr}; expected host:port" >&2
    exit 2
  fi
  echo "${host}" "${port}"
}

wait_for_file_listener() {
  local host="$1"
  local port="$2"
  local attempts=$(( SERVER_STARTUP_TIMEOUT_SEC * 10 ))
  if (( attempts < 1 )); then
    attempts=1
  fi
  for _ in $(seq 1 "${attempts}"); do
    if (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      exec 3>&-
      exec 3<&-
      return 0
    fi
    if ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
      echo "server exited before becoming ready; log: ${SERVER_LOG}" >&2
      tail -n 50 "${SERVER_LOG}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
  return 1
}

start_server() {
  local host port
  read -r host port < <(split_host_port "${SERVER_URL}")
  mkdir -p "${SERVER_IN}" "${SERVER_OUT}" "${SERVER_KEYS}"
  echo "Starting server in background..."
  ./pinch -in "${SERVER_IN}" -out "${SERVER_OUT}" -keys "${SERVER_KEYS}" >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  SERVER_APP_PID="${SERVER_PID}"

  if wait_for_file_listener "${host}" "${port}"; then
    echo "Server is ready at ${SERVER_URL}"
    return
  fi
  echo "server did not become ready in ${SERVER_STARTUP_TIMEOUT_SEC}s; log: ${SERVER_LOG}" >&2
  tail -n 50 "${SERVER_LOG}" >&2 || true
  exit 1
}

start_server_perf() {
  if [[ -z "${SERVER_APP_PID}" ]]; then
    echo "missing server pid for async perf record" >&2
    exit 1
  fi
  echo "Recording async server perf profile (sudo may prompt)..."
  sudo perf record -o "${SERVER_PERF_DATA}" -F "${PERF_FREQ}" -g --call-graph fp -p "${SERVER_APP_PID}" >/tmp/pinch-server-perf.log 2>&1 &
  SERVER_PERF_PID=$!
}

trap cleanup_server EXIT INT TERM
start_server

echo "Cleaning prior benchmark output..."
rm -rf "${OUT_ROOT}/" "${MANIFEST_PATH}"*

echo "Preparing manifest..."
TRANSFER_CMD=(./pinch cli "${SERVER_URL}" transfer -o "${MANIFEST_PATH}" -s "${SOURCE_DIRECTORY}")
if [[ -n "${ENCRYPT_MODE}" ]]; then
  TRANSFER_CMD+=(--encrypt "${ENCRYPT_MODE}")
fi
"${TRANSFER_CMD[@]}"

START_CMD=(./pinch cli "${SERVER_URL}" start --manifest "${MANIFEST_PATH}" --out-root "${OUT_ROOT}" --concurrency "${CONCURRENCY}")
if [[ "${USE_ZEROCOPY}" == "true" ]]; then
  START_CMD+=(--zerocopy)
fi
if [[ -n "${ENCRYPT_MODE}" ]]; then
  START_CMD+=(--encrypt "${ENCRYPT_MODE}")
fi

if [[ "$FLAMEGRAPH" != "true" ]]; then
  echo "Running timed start benchmark..."
  { time "${START_CMD[@]}" 2>/dev/null | awk '!/^start-file: /'; } 2>&1
  echo "Flamegraph disabled. Re-run with --flamegraph to generate ${FLAMEGRAPH_SVG} and ${SERVER_FLAMEGRAPH_SVG}."
  exit 0
fi

require_cmd perf

STACKCOLLAPSE="stackcollapse-perf.pl"
FLAMEGRAPH_PL="flamegraph.pl"
if [[ -n "$FLAMEGRAPH_DIR" ]]; then
  STACKCOLLAPSE="${FLAMEGRAPH_DIR%/}/stackcollapse-perf.pl"
  FLAMEGRAPH_PL="${FLAMEGRAPH_DIR%/}/flamegraph.pl"
fi

if ! command -v "$STACKCOLLAPSE" >/dev/null 2>&1 && [[ ! -x "$STACKCOLLAPSE" ]]; then
  echo "missing stackcollapse-perf.pl; set --flamegraph-dir or add it to PATH" >&2
  exit 1
fi
if ! command -v "$FLAMEGRAPH_PL" >/dev/null 2>&1 && [[ ! -x "$FLAMEGRAPH_PL" ]]; then
  echo "missing flamegraph.pl; set --flamegraph-dir or add it to PATH" >&2
  exit 1
fi

PERF_TXT="${PERF_DATA}.txt"
PERF_FOLDED="${PERF_DATA}.folded"
SERVER_PERF_TXT="${SERVER_PERF_DATA}.txt"
SERVER_PERF_FOLDED="${SERVER_PERF_DATA}.folded"

echo "Recording perf profile (sudo may prompt)..."
start_server_perf
sudo perf record -o "${PERF_DATA}" -F "${PERF_FREQ}" -g --call-graph fp -- "${START_CMD[@]}"
stop_server_perf

echo "Generating client flamegraph..."
sudo perf script -i "${PERF_DATA}" > "${PERF_TXT}"
"${STACKCOLLAPSE}" "${PERF_TXT}" > "${PERF_FOLDED}"
"${FLAMEGRAPH_PL}" "${PERF_FOLDED}" > "${FLAMEGRAPH_SVG}"

echo "Generating server flamegraph..."
sudo perf script -i "${SERVER_PERF_DATA}" > "${SERVER_PERF_TXT}"
"${STACKCOLLAPSE}" "${SERVER_PERF_TXT}" > "${SERVER_PERF_FOLDED}"
"${FLAMEGRAPH_PL}" "${SERVER_PERF_FOLDED}" > "${SERVER_FLAMEGRAPH_SVG}"

echo "Flamegraph written to: ${FLAMEGRAPH_SVG}"
echo "Server flamegraph written to: ${SERVER_FLAMEGRAPH_SVG}"
echo "Perf data written to: ${PERF_DATA}"
echo "Server perf data written to: ${SERVER_PERF_DATA}"

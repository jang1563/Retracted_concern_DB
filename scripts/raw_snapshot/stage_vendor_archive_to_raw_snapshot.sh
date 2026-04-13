#!/bin/bash
set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <vendor_archive_root> <raw_snapshot_root> <snapshot_label> [copy|symlink]" >&2
  exit 1
fi

VENDOR_ROOT="$1"
RAW_ROOT="$2"
SNAPSHOT_LABEL="$3"
MODE="${4:-copy}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$REPO_ROOT"
export PYTHONPATH=src

CMD=(
  "$PYTHON_BIN" -m life_science_integrity_benchmark.cli
  stage-vendor-archive
  --vendor-root "$VENDOR_ROOT"
  --raw-root "$RAW_ROOT"
  --snapshot-label "$SNAPSHOT_LABEL"
  --mode "$MODE"
)

if [ "${ALLOW_MISSING_CROSSREF:-0}" = "1" ]; then
  CMD+=(--allow-missing-crossref)
fi

"${CMD[@]}"

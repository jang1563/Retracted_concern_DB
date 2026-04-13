#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <vendor_archive_root> <snapshot_label> [release_dir]" >&2
  exit 1
fi

VENDOR_ROOT="$1"
SNAPSHOT_LABEL="$2"
RELEASE_DIR="${3:-$VENDOR_ROOT/_validation}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$RELEASE_DIR"

cd "$REPO_ROOT"
export PYTHONPATH=src

CMD=(
  "$PYTHON_BIN" -m life_science_integrity_benchmark.cli
  --release-dir "$RELEASE_DIR"
  validate-vendor-archive
  --vendor-root "$VENDOR_ROOT"
  --snapshot-label "$SNAPSHOT_LABEL"
)

if [ "${ALLOW_MISSING_CROSSREF:-0}" = "1" ]; then
  CMD+=(--allow-missing-crossref)
fi

"${CMD[@]}"

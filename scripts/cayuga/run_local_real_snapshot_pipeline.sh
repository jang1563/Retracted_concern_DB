#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <local_raw_snapshot_root> <work_root> [snapshot_id]" >&2
  exit 1
fi

LOCAL_RAW_ROOT="$1"
WORK_ROOT="$2"
SNAPSHOT_ID="${3:-local_dryrun_$(date +%Y%m%d_%H%M%S)}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNTIME_ROOT="$WORK_ROOT/runtime_root"
RELEASE_DIR="$WORK_ROOT/release"
SITE_DIR="$WORK_ROOT/site"
CANONICAL_SOURCE_DIR="$RUNTIME_ROOT/data/normalized/$SNAPSHOT_ID/canonical"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$WORK_ROOT" "$RUNTIME_ROOT" "$RELEASE_DIR" "$SITE_DIR"

"$SCRIPT_DIR/check_local_raw_snapshot.sh" "$LOCAL_RAW_ROOT"
"$SCRIPT_DIR/inventory_local_raw_snapshot.sh" "$LOCAL_RAW_ROOT" "$WORK_ROOT/inventory" >/dev/null

cd "$REPO_ROOT"
export PYTHONPATH=src

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --root-dir "$RUNTIME_ROOT" \
  register-snapshot \
  --snapshot-id "$SNAPSHOT_ID" \
  --raw-root "$LOCAL_RAW_ROOT" \
  --source-family openalex_notices

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli --root-dir "$RUNTIME_ROOT" ingest-snapshot --snapshot-id "$SNAPSHOT_ID" --collector openalex_bulk
"$PYTHON_BIN" -m life_science_integrity_benchmark.cli --root-dir "$RUNTIME_ROOT" ingest-snapshot --snapshot-id "$SNAPSHOT_ID" --collector local_notice_export
"$PYTHON_BIN" -m life_science_integrity_benchmark.cli --root-dir "$RUNTIME_ROOT" ingest-snapshot --snapshot-id "$SNAPSHOT_ID" --collector pubmed_index
"$PYTHON_BIN" -m life_science_integrity_benchmark.cli --root-dir "$RUNTIME_ROOT" materialize-canonical --snapshot-id "$SNAPSHOT_ID"

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --root-dir "$RUNTIME_ROOT" \
  --release-dir "$RELEASE_DIR" \
  validate-snapshot \
  --snapshot-id "$SNAPSHOT_ID"

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --source-dir "$CANONICAL_SOURCE_DIR" \
  --release-dir "$RELEASE_DIR" \
  --site-dir "$SITE_DIR" \
  build-core

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --source-dir "$CANONICAL_SOURCE_DIR" \
  --release-dir "$RELEASE_DIR" \
  build-splits

"$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --source-dir "$CANONICAL_SOURCE_DIR" \
  --release-dir "$RELEASE_DIR" \
  audit-leakage

echo "snapshot_id=$SNAPSHOT_ID"
echo "runtime_root=$RUNTIME_ROOT"
echo "release_dir=$RELEASE_DIR"
echo "site_dir=$SITE_DIR"
echo "canonical_source_dir=$CANONICAL_SOURCE_DIR"

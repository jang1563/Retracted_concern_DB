#!/bin/bash
set -euo pipefail

if [ "$#" -lt 4 ] || [ "$#" -gt 6 ]; then
  echo "usage: $0 <vendor_archive_root> <raw_snapshot_root> <work_root> <snapshot_label> [copy|symlink] [snapshot_id]" >&2
  exit 1
fi

VENDOR_ROOT="$1"
RAW_ROOT="$2"
WORK_ROOT="$3"
SNAPSHOT_LABEL="$4"
MODE="${5:-copy}"
SNAPSHOT_ID="${6:-vendor_dryrun_$(date +%Y%m%d_%H%M%S)}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ "${ALLOW_MISSING_CROSSREF:-0}" = "1" ]; then
  export ALLOW_MISSING_CROSSREF=1
fi

/bin/bash "$SCRIPT_DIR/check_vendor_archive_snapshot.sh" "$VENDOR_ROOT" "$SNAPSHOT_LABEL" "$WORK_ROOT/vendor_validation" >/dev/null
/bin/bash "$SCRIPT_DIR/stage_vendor_archive_to_raw_snapshot.sh" "$VENDOR_ROOT" "$RAW_ROOT" "$SNAPSHOT_LABEL" "$MODE" >/dev/null
/bin/bash "$REPO_ROOT/scripts/cayuga/check_local_raw_snapshot.sh" "$RAW_ROOT" >/dev/null
/bin/bash "$REPO_ROOT/scripts/cayuga/run_local_real_snapshot_pipeline.sh" "$RAW_ROOT" "$WORK_ROOT" "$SNAPSHOT_ID"

echo "vendor_root=$VENDOR_ROOT"
echo "raw_root=$RAW_ROOT"
echo "work_root=$WORK_ROOT"
echo "snapshot_label=$SNAPSHOT_LABEL"
echo "snapshot_id=$SNAPSHOT_ID"

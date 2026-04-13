#!/bin/bash
set -euo pipefail

if [ "$#" -lt 4 ] || [ "$#" -gt 6 ]; then
  echo "usage: $0 <vendor_archive_root> <raw_snapshot_root> <work_root> <snapshot_label> [copy|symlink] [snapshot_id]" >&2
  exit 1
fi

export RAW_SNAPSHOT_COLLECTION_PROFILE="open-data-only"
export CROSSREF_SOURCE_MODE="skip"
export ALLOW_MISSING_CROSSREF=1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

VENDOR_ROOT="$1"
RAW_ROOT="$2"
WORK_ROOT="$3"
SNAPSHOT_LABEL="$4"
MODE="${5:-symlink}"
SNAPSHOT_ID="${6:-}"

if [ "$MODE" != "copy" ] && [ "$MODE" != "symlink" ]; then
  echo "mode must be copy or symlink" >&2
  exit 1
fi

CMD=(
  /bin/bash "$SCRIPT_DIR/collect_monthly_freeze.sh"
  "$VENDOR_ROOT"
  "$RAW_ROOT"
  "$WORK_ROOT"
  "$SNAPSHOT_LABEL"
  "$MODE"
)

if [ -n "$SNAPSHOT_ID" ]; then
  CMD+=("$SNAPSHOT_ID")
fi

"${CMD[@]}"

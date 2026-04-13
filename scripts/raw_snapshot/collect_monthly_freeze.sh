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
SNAPSHOT_ID="${6:-monthly_freeze_$(date +%Y%m%d_%H%M%S)}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"
PYTHON_BIN="${PYTHON_BIN:-python3}"
OPENALEX_COLLECTION_MODE="${OPENALEX_COLLECTION_MODE:-works-only}"
RETRACTION_WATCH_COLLECTION_DATE="${RETRACTION_WATCH_COLLECTION_DATE:-$("$PYTHON_BIN" - "$SNAPSHOT_LABEL" <<'PY'
import sys
from calendar import monthrange

label = sys.argv[1]
period = label[:-7] if label.endswith("-freeze") else label
year, month = period.split("-")
last_day = monthrange(int(year), int(month))[1]
print(f"{int(year):04d}-{int(month):02d}-{last_day:02d}")
PY
)}"
CROSSREF_SOURCE_MODE="${CROSSREF_SOURCE_MODE:-metadata_plus}"

/bin/bash "$SCRIPT_DIR/check_collection_runtime.sh"
/bin/bash "$SCRIPT_DIR/collect_openalex_snapshot.sh" "$VENDOR_ROOT" "$SNAPSHOT_LABEL" "$OPENALEX_COLLECTION_MODE"
if [ "$CROSSREF_SOURCE_MODE" = "metadata_plus" ]; then
  /bin/bash "$SCRIPT_DIR/collect_crossref_monthly_snapshot.sh" "$VENDOR_ROOT" "$SNAPSHOT_LABEL"
elif [ "$CROSSREF_SOURCE_MODE" = "skip" ]; then
  :
elif [ "$CROSSREF_SOURCE_MODE" = "public_data_file" ]; then
  echo "CROSSREF_SOURCE_MODE=public_data_file requires a separately acquired public data file staged under vendor_archive/crossref/<YYYY-MM>/all.json.tar.gz" >&2
else
  echo "unsupported CROSSREF_SOURCE_MODE: $CROSSREF_SOURCE_MODE" >&2
  exit 1
fi
/bin/bash "$SCRIPT_DIR/collect_retraction_watch_csv.sh" "$VENDOR_ROOT" "$RETRACTION_WATCH_COLLECTION_DATE"
/bin/bash "$SCRIPT_DIR/collect_pubmed_baseline_updatefiles.sh" "$VENDOR_ROOT" "$SNAPSHOT_LABEL"
/bin/bash "$SCRIPT_DIR/run_vendor_archive_pipeline.sh" "$VENDOR_ROOT" "$RAW_ROOT" "$WORK_ROOT" "$SNAPSHOT_LABEL" "$MODE" "$SNAPSHOT_ID"

echo "vendor_root=$VENDOR_ROOT"
echo "raw_root=$RAW_ROOT"
echo "work_root=$WORK_ROOT"
echo "snapshot_label=$SNAPSHOT_LABEL"
echo "snapshot_id=$SNAPSHOT_ID"
echo "openalex_collection_mode=$OPENALEX_COLLECTION_MODE"
echo "crossref_source_mode=$CROSSREF_SOURCE_MODE"
echo "retraction_watch_collection_date=$RETRACTION_WATCH_COLLECTION_DATE"

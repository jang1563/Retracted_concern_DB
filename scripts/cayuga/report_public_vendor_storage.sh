#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
VENDOR_ROOT="$RUN_ROOT/vendor_archive"
RAW_ROOT="$RUN_ROOT/raw/public_open_data_snapshot"
WORK_ROOT="$RUN_ROOT/artifacts/public_vendor_collection_work"
ART_ROOT="$RUN_ROOT/artifacts/public_vendor_collection"

report_path() {
  local label="$1"
  local path="$2"
  if [ -e "$path" ]; then
    local size
    size="$(du -sh "$path" 2>/dev/null | awk '{print $1}')"
    echo "size\t$label\t$size\t$path"
  else
    echo "missing\t$label\t$path"
  fi
}

count_symlinks() {
  local label="$1"
  local path="$2"
  if [ -d "$path" ]; then
    local count
    count="$(find "$path" -type l | wc -l | tr -d ' ')"
    echo "symlinks\t$label\t$count"
  fi
}

echo "generated_at=$(date '+%F %T %Z')"
report_path "vendor_archive" "$VENDOR_ROOT"
report_path "raw_public_open_data_snapshot" "$RAW_ROOT"
report_path "public_vendor_collection_work" "$WORK_ROOT"
report_path "public_vendor_collection_artifacts" "$ART_ROOT"

for bucket in openalex official_notices pubmed; do
  report_path "raw_bucket:$bucket" "$RAW_ROOT/$bucket"
  count_symlinks "raw_bucket:$bucket" "$RAW_ROOT/$bucket"
done

#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
RAW_ROOT="$RUN_ROOT/raw/real_snapshot"
STATUS=0

is_allowed_sidecar() {
  local label="$1"
  local file_name="$2"
  case "$file_name" in
    source_versions.json|sha256_manifest.tsv|fetch.log)
      return 0
      ;;
  esac
  if [ "$label" = "openalex" ]; then
    case "$file_name" in
      manifest|LICENSE.txt|RELEASE_NOTES.txt)
        return 0
        ;;
    esac
  fi
  return 1
}

check_dir() {
  local label="$1"
  local path="$2"
  if [ ! -d "$path" ]; then
    echo "missing_dir	$label	$path"
    STATUS=1
    return
  fi

  local ingest_count
  local sidecar_count
  local unsupported_count
  ingest_count=0
  sidecar_count=0
  unsupported_count=0

  while IFS= read -r file_path; do
    [ -n "$file_path" ] || continue
    case "$label" in
      openalex)
        case "$file_path" in
          *.jsonl|*.jsonl.gz|*.gz)
            ingest_count=$((ingest_count + 1))
            continue
            ;;
        esac
        ;;
      official_notices)
        case "$file_path" in
          *.jsonl|*.jsonl.gz|*.csv|*.csv.gz)
            ingest_count=$((ingest_count + 1))
            continue
            ;;
        esac
        ;;
      pubmed)
        case "$file_path" in
          *.jsonl|*.jsonl.gz|*.csv|*.csv.gz|*.xml|*.xml.gz)
            ingest_count=$((ingest_count + 1))
            continue
            ;;
        esac
        ;;
    esac
    if is_allowed_sidecar "$label" "$(basename "$file_path")"; then
      sidecar_count=$((sidecar_count + 1))
      continue
    fi
    unsupported_count=$((unsupported_count + 1))
    echo "unsupported_file	$label	$file_path"
  done < <(find "$path" \( -type f -o -type l \) | sort)

  echo "ingest_file_count	$label	$ingest_count	$path"
  echo "sidecar_file_count	$label	$sidecar_count	$path"
  echo "unsupported_count	$label	$unsupported_count	$path"
  if [ "$ingest_count" -eq 0 ] || [ "$unsupported_count" -gt 0 ]; then
    STATUS=1
  fi
}

check_dir "openalex" "$RAW_ROOT/openalex"
check_dir "official_notices" "$RAW_ROOT/official_notices"
check_dir "pubmed" "$RAW_ROOT/pubmed"

if [ "$STATUS" -ne 0 ]; then
  echo "real_snapshot_ready=no"
  exit 1
fi

echo "real_snapshot_ready=yes"

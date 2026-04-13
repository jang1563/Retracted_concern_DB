#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <local_raw_snapshot_root>" >&2
  exit 1
fi

RAW_ROOT="$1"
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
  shift 2
  local patterns=("$@")

  if [ ! -d "$path" ]; then
    echo "missing_dir	$label	$path"
    STATUS=1
    return
  fi

  local total_count
  total_count="$(find "$path" \( -type f -o -type l \) | wc -l | tr -d ' ')"
  echo "file_count	$label	$total_count	$path"
  if [ "$total_count" -eq 0 ]; then
    STATUS=1
  fi

  local unsupported_count
  local ingest_count
  local sidecar_count
  unsupported_count=0
  ingest_count=0
  sidecar_count=0
  while IFS= read -r file_path; do
    [ -n "$file_path" ] || continue
    local supported=0
    local pattern
    for pattern in "${patterns[@]}"; do
      case "$file_path" in
        *"$pattern")
          supported=1
          break
          ;;
      esac
    done
    if [ "$supported" -eq 1 ]; then
      ingest_count=$((ingest_count + 1))
      continue
    fi
    if is_allowed_sidecar "$label" "$(basename "$file_path")"; then
      sidecar_count=$((sidecar_count + 1))
      continue
    fi
    if [ "$supported" -eq 0 ]; then
      echo "unsupported_file	$label	$file_path"
      unsupported_count=$((unsupported_count + 1))
    fi
  done < <(find "$path" \( -type f -o -type l \) | sort)

  echo "ingest_file_count	$label	$ingest_count"
  echo "sidecar_file_count	$label	$sidecar_count"
  echo "unsupported_count	$label	$unsupported_count"
  if [ "$unsupported_count" -gt 0 ] || [ "$ingest_count" -eq 0 ]; then
    STATUS=1
  fi
}

if [ ! -d "$RAW_ROOT" ]; then
  echo "missing_root	$RAW_ROOT"
  exit 1
fi

check_dir "openalex" "$RAW_ROOT/openalex" ".jsonl" ".jsonl.gz" ".gz"
check_dir "official_notices" "$RAW_ROOT/official_notices" ".jsonl" ".jsonl.gz" ".csv" ".csv.gz"
check_dir "pubmed" "$RAW_ROOT/pubmed" ".jsonl" ".jsonl.gz" ".csv" ".csv.gz" ".xml" ".xml.gz"

if [ "$STATUS" -ne 0 ]; then
  echo "local_raw_snapshot_ready=no"
  exit 1
fi

echo "local_raw_snapshot_ready=yes"

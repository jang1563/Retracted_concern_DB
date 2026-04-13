#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <local_raw_snapshot_root> [output_dir]" >&2
  exit 1
fi

RAW_ROOT="$1"
OUTPUT_DIR="${2:-$RAW_ROOT/inventory}"
DETAILS_PATH="$OUTPUT_DIR/file_inventory.tsv"
SUMMARY_PATH="$OUTPUT_DIR/summary.txt"

if [ ! -d "$RAW_ROOT" ]; then
  echo "raw snapshot root does not exist: $RAW_ROOT" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

printf "bucket\trelative_path\tsize_bytes\tsha256\n" > "$DETAILS_PATH"

inventory_bucket() {
  local bucket="$1"
  local path="$RAW_ROOT/$bucket"
  if [ ! -d "$path" ]; then
    return
  fi

  while IFS= read -r file_path; do
    [ -n "$file_path" ] || continue
    local rel_path
    local size_bytes
    local sha256
    rel_path="${file_path#$RAW_ROOT/}"
    size_bytes="$(wc -c < "$file_path" | tr -d ' ')"
    sha256="$(shasum -a 256 "$file_path" | awk '{print $1}')"
    printf "%s\t%s\t%s\t%s\n" "$bucket" "$rel_path" "$size_bytes" "$sha256" >> "$DETAILS_PATH"
  done < <(find "$path" \( -type f -o -type l \) | sort)
}

inventory_bucket "openalex"
inventory_bucket "official_notices"
inventory_bucket "pubmed"

{
  echo "raw_root=$RAW_ROOT"
  echo "generated_at=$(date '+%F %T')"
  echo
  for bucket in openalex official_notices pubmed; do
    bucket_dir="$RAW_ROOT/$bucket"
    if [ -d "$bucket_dir" ]; then
      count="$(find "$bucket_dir" \( -type f -o -type l \) | wc -l | tr -d ' ')"
      total_bytes="$(find "$bucket_dir" \( -type f -o -type l \) -exec wc -c {} + 2>/dev/null | tail -n 1 | awk '{print $1}')"
      total_bytes="${total_bytes:-0}"
      echo "bucket=$bucket files=$count bytes=$total_bytes"
    else
      echo "bucket=$bucket files=missing bytes=missing"
    fi
  done
  echo
  echo "details_tsv=$DETAILS_PATH"
} > "$SUMMARY_PATH"

cat "$SUMMARY_PATH"

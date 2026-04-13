#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <classification_tsv> [output_tsv]" >&2
  exit 1
fi

CLASSIFICATION_PATH="$1"
OUTPUT_PATH="${2:-$(dirname "$CLASSIFICATION_PATH")/classification_overrides.tsv}"

if [ ! -f "$CLASSIFICATION_PATH" ]; then
  echo "classification file does not exist: $CLASSIFICATION_PATH" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
printf "relative_path\tbucket\treason\n" > "$OUTPUT_PATH"

tail -n +2 "$CLASSIFICATION_PATH" | while IFS=$'\t' read -r bucket reason rel_path; do
  [ -n "$bucket" ] || continue
  if [ "$bucket" = "unknown" ]; then
    printf "%s\t\tmanual_override\n" "$rel_path" >> "$OUTPUT_PATH"
  fi
done

echo "$OUTPUT_PATH"

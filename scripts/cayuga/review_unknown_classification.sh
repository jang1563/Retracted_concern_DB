#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <mixed_source_root> [classification_tsv]" >&2
  exit 1
fi

SOURCE_ROOT="$1"
CLASSIFICATION_PATH="${2:-$SOURCE_ROOT/source_classification.tsv}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -d "$SOURCE_ROOT" ]; then
  echo "mixed source root does not exist: $SOURCE_ROOT" >&2
  exit 1
fi

if [ ! -f "$CLASSIFICATION_PATH" ]; then
  "$SCRIPT_DIR/classify_mixed_sources.sh" "$SOURCE_ROOT" "$CLASSIFICATION_PATH" >/dev/null
fi

sample_file_text() {
  local file_path="$1"
  case "$file_path" in
    *.gz)
      gzip -cd "$file_path" 2>/dev/null | sed -n '1,5p'
      ;;
    *)
      sed -n '1,5p' "$file_path" 2>/dev/null
      ;;
  esac
}

found_unknown=0
tail -n +2 "$CLASSIFICATION_PATH" | while IFS=$'\t' read -r bucket reason rel_path; do
  [ -n "$bucket" ] || continue
  bucket="${bucket%$'\r'}"
  reason="${reason%$'\r'}"
  rel_path="${rel_path%$'\r'}"
  if [ "$bucket" != "unknown" ]; then
    continue
  fi
  found_unknown=1
  abs_path="$SOURCE_ROOT/$rel_path"
  echo "relative_path=$rel_path"
  echo "reason=$reason"
  echo "sample_begin"
  sample_file_text "$abs_path"
  echo "sample_end"
  echo
done

if ! grep -q '^unknown' "$CLASSIFICATION_PATH"; then
  echo "no_unknown_files"
fi

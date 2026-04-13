#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 <mixed_source_root> <local_raw_snapshot_root> [copy|symlink] [classification_tsv] [override_tsv]" >&2
  exit 1
fi

SOURCE_ROOT="$1"
RAW_ROOT="$2"
MODE="${3:-copy}"
CLASSIFICATION_PATH="${4:-$SOURCE_ROOT/source_classification.tsv}"
OVERRIDE_PATH="${5:-}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ "$MODE" != "copy" ] && [ "$MODE" != "symlink" ]; then
  echo "mode must be copy or symlink" >&2
  exit 1
fi

"$SCRIPT_DIR/scaffold_local_raw_snapshot.sh" "$RAW_ROOT" >/dev/null
"$SCRIPT_DIR/classify_mixed_sources.sh" "$SOURCE_ROOT" "$CLASSIFICATION_PATH" >/dev/null

if [ -n "$OVERRIDE_PATH" ]; then
  "$SCRIPT_DIR/apply_classification_overrides.sh" "$CLASSIFICATION_PATH" "$OVERRIDE_PATH" "$CLASSIFICATION_PATH" >/dev/null
fi

flatten_name() {
  local rel_path="$1"
  printf "%s" "$rel_path" | sed 's#^\./##' | sed 's#/#__#g'
}

stage_one() {
  local bucket="$1"
  local rel_path="$2"
  local src="$SOURCE_ROOT/$rel_path"
  local dst_name
  local dst
  dst_name="$(flatten_name "$rel_path")"
  dst="$RAW_ROOT/$bucket/$dst_name"

  if [ -e "$dst" ]; then
    if cmp -s "$src" "$dst"; then
      printf "skip_same\t%s\t%s\n" "$bucket" "$rel_path"
      return
    fi
    echo "target exists with different contents: $dst" >&2
    exit 1
  fi

  if [ "$MODE" = "copy" ]; then
    cp "$src" "$dst"
  else
    ln -s "$src" "$dst"
  fi
  printf "staged\t%s\t%s\t%s\n" "$bucket" "$rel_path" "$dst"
}

tail -n +2 "$CLASSIFICATION_PATH" | while IFS=$'\t' read -r bucket reason rel_path; do
  [ -n "$bucket" ] || continue
  bucket="${bucket%$'\r'}"
  reason="${reason%$'\r'}"
  rel_path="${rel_path%$'\r'}"
  if [ "$bucket" = "unknown" ]; then
    printf "unclassified\t%s\t%s\n" "$reason" "$rel_path"
    continue
  fi
  stage_one "$bucket" "$rel_path"
done

"$SCRIPT_DIR/check_local_raw_snapshot.sh" "$RAW_ROOT"

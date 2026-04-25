#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <vendor_archive_root> [collection_date]" >&2
  exit 1
fi

VENDOR_ROOT="$1"
COLLECTION_DATE="${2:-$(date +%F)}"
TARGET_DIR="$VENDOR_ROOT/retraction_watch/$COLLECTION_DATE"
REPO_URL="${RETRACTION_WATCH_REPO_URL:-https://gitlab.com/crossref/retraction-watch-data.git}"
CLONE_DIR="$TARGET_DIR/repo"
GIT_BIN="${GIT_BIN:-git}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$TARGET_DIR"

if [ -d "$CLONE_DIR/.git" ]; then
  "$GIT_BIN" -C "$CLONE_DIR" fetch origin
else
  "$GIT_BIN" clone "$REPO_URL" "$CLONE_DIR"
fi

if [ -n "${RETRACTION_WATCH_GIT_REF:-}" ]; then
  TARGET_COMMIT="$RETRACTION_WATCH_GIT_REF"
else
  TARGET_COMMIT="$("$GIT_BIN" -C "$CLONE_DIR" rev-list -n 1 --before="$COLLECTION_DATE 23:59:59 +0000" --all)"
fi
if [ -z "$TARGET_COMMIT" ]; then
  echo "could not locate Retraction Watch git commit at or before $COLLECTION_DATE" >&2
  exit 1
fi
"$GIT_BIN" -C "$CLONE_DIR" reset --hard "$TARGET_COMMIT"

SOURCE_CSV="$(find "$CLONE_DIR" -maxdepth 3 -type f \( -name '*.csv' -o -name '*.csv.gz' \) | sort | sed -n '1p')"
if [ -z "$SOURCE_CSV" ]; then
  echo "could not locate retraction watch csv in $CLONE_DIR" >&2
  exit 1
fi

case "$SOURCE_CSV" in
  *.gz)
    cp "$SOURCE_CSV" "$TARGET_DIR/retraction_watch.csv.gz"
    ;;
  *)
    cp "$SOURCE_CSV" "$TARGET_DIR/retraction_watch.csv"
    ;;
esac

GIT_SHA="$("$GIT_BIN" -C "$CLONE_DIR" rev-parse HEAD)"

rm -rf "$CLONE_DIR"

"$PYTHON_BIN" - "$TARGET_DIR" "$COLLECTION_DATE" "$REPO_URL" "$GIT_SHA" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

target_dir = Path(sys.argv[1])
collection_date = sys.argv[2]
repo_url = sys.argv[3]
git_sha = sys.argv[4]
payload = {
    "snapshot_label": collection_date,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "batches": [
        {
            "source_name": "Retraction Watch",
            "source_url": repo_url,
            "git_sha": git_sha,
        }
    ],
}
(target_dir / "source_versions.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY

{
  printf "relative_path\tsize_bytes\tsha256\n"
  find "$TARGET_DIR" -type f ! -name 'sha256_manifest.tsv' | sort | while IFS= read -r file_path; do
    rel_path="${file_path#$TARGET_DIR/}"
    size_bytes="$(wc -c < "$file_path" | tr -d ' ')"
    sha256="$(shasum -a 256 "$file_path" | awk '{print $1}')"
    printf "%s\t%s\t%s\n" "$rel_path" "$size_bytes" "$sha256"
  done
} > "$TARGET_DIR/sha256_manifest.tsv"

echo "$TARGET_DIR"

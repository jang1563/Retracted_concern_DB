#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <vendor_archive_root> <snapshot_label>" >&2
  exit 1
fi

VENDOR_ROOT="$1"
SNAPSHOT_LABEL="$2"
PERIOD_KEY="${SNAPSHOT_LABEL%-freeze}"
YEAR="${PERIOD_KEY%-*}"
MONTH="${PERIOD_KEY#*-}"
TARGET_DIR="$VENDOR_ROOT/crossref/$PERIOD_KEY"
SNAPSHOT_URL="https://api.crossref.org/snapshots/monthly/$YEAR/$MONTH/all.json.tar.gz"
TOKEN="${CROSSREF_PLUS_TOKEN:-}"
CURL_BIN="${CURL_BIN:-curl}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ -z "$TOKEN" ]; then
  echo "CROSSREF_PLUS_TOKEN must be set" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"

"$CURL_BIN" -fsSLI -H "Crossref-Plus-API-Token: Bearer $TOKEN" "$SNAPSHOT_URL" > "$TARGET_DIR/snapshot_headers.txt"
"$CURL_BIN" -fsSL -H "Crossref-Plus-API-Token: Bearer $TOKEN" "$SNAPSHOT_URL" -o "$TARGET_DIR/all.json.tar.gz"

"$PYTHON_BIN" - "$TARGET_DIR" "$SNAPSHOT_LABEL" "$SNAPSHOT_URL" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

target_dir = Path(sys.argv[1])
snapshot_label = sys.argv[2]
snapshot_url = sys.argv[3]
payload = {
    "snapshot_label": snapshot_label,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "batches": [
        {
            "source_name": "Crossref Metadata Plus",
            "source_url": snapshot_url,
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

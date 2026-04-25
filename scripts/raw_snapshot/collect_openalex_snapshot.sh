#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <vendor_archive_root> <snapshot_label> [works-only|full]" >&2
  exit 1
fi

VENDOR_ROOT="$1"
SNAPSHOT_LABEL="$2"
MODE="${3:-works-only}"
PERIOD_KEY="${SNAPSHOT_LABEL%-freeze}"
TARGET_DIR="$VENDOR_ROOT/openalex/$PERIOD_KEY"
AWS_BIN="${AWS_BIN:-aws}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
AWS_PROGRESS_FLAGS=()
FREEZE_END="$("$PYTHON_BIN" - "$SNAPSHOT_LABEL" <<'PY'
import sys
from calendar import monthrange

label = sys.argv[1]
period = label[:-7] if label.endswith("-freeze") else label
year, month = period.split("-")
last_day = monthrange(int(year), int(month))[1]
print(f"{int(year):04d}-{int(month):02d}-{last_day:02d}")
PY
)"

if [ "${AWS_NO_PROGRESS:-1}" = "1" ]; then
  AWS_PROGRESS_FLAGS+=(--no-progress)
fi

if [ "$MODE" != "works-only" ] && [ "$MODE" != "full" ]; then
  echo "mode must be works-only or full" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"

if [ "$MODE" = "works-only" ]; then
  "$AWS_BIN" s3 sync "s3://openalex/data/works" "$TARGET_DIR/data/works" --no-sign-request "${AWS_PROGRESS_FLAGS[@]}"
  "$AWS_BIN" s3 cp "s3://openalex/LICENSE.txt" "$TARGET_DIR/LICENSE.txt" --no-sign-request "${AWS_PROGRESS_FLAGS[@]}" || true
  "$AWS_BIN" s3 cp "s3://openalex/RELEASE_NOTES.txt" "$TARGET_DIR/RELEASE_NOTES.txt" --no-sign-request "${AWS_PROGRESS_FLAGS[@]}" || true
else
  "$AWS_BIN" s3 sync "s3://openalex" "$TARGET_DIR" --no-sign-request "${AWS_PROGRESS_FLAGS[@]}"
fi

PRUNED_FUTURE_PARTITIONS=0
if [ -d "$TARGET_DIR/data/works" ]; then
  while IFS= read -r partition_dir; do
    partition_date="${partition_dir##*updated_date=}"
    if [[ "$partition_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "$partition_date" > "$FREEZE_END" ]]; then
      rm -rf "$partition_dir"
      PRUNED_FUTURE_PARTITIONS=$((PRUNED_FUTURE_PARTITIONS + 1))
    fi
  done < <(find "$TARGET_DIR/data/works" -type d -name 'updated_date=*' | sort)
fi

"$PYTHON_BIN" - "$TARGET_DIR" "$SNAPSHOT_LABEL" "$MODE" "$FREEZE_END" "$PRUNED_FUTURE_PARTITIONS" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

target_dir = Path(sys.argv[1])
snapshot_label = sys.argv[2]
mode = sys.argv[3]
freeze_end = sys.argv[4]
pruned_future_partitions = int(sys.argv[5])
payload = {
    "snapshot_label": snapshot_label,
    "freeze_end": freeze_end,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "batches": [
        {
            "source_name": "OpenAlex works snapshot",
            "mode": mode,
            "source_url": "s3://openalex/data/works" if mode == "works-only" else "s3://openalex",
            "pruned_future_partitions": pruned_future_partitions,
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

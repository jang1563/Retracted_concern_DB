#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <classification_tsv> <override_tsv> [output_tsv]" >&2
  exit 1
fi

CLASSIFICATION_PATH="$1"
OVERRIDE_PATH="$2"
OUTPUT_PATH="${3:-$(dirname "$CLASSIFICATION_PATH")/source_classification.merged.tsv}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/common_python_env.sh"
lsib_require_python_bin "${PYTHON_BIN:-}"

if [ ! -f "$CLASSIFICATION_PATH" ]; then
  echo "classification file does not exist: $CLASSIFICATION_PATH" >&2
  exit 1
fi

if [ ! -f "$OVERRIDE_PATH" ]; then
  echo "override file does not exist: $OVERRIDE_PATH" >&2
  exit 1
fi

"$PYTHON_BIN" - "$CLASSIFICATION_PATH" "$OVERRIDE_PATH" "$OUTPUT_PATH" <<'PY'
import csv
import sys
from pathlib import Path

classification_path = Path(sys.argv[1])
override_path = Path(sys.argv[2])
output_path = Path(sys.argv[3])

allowed = {"openalex", "official_notices", "pubmed", "unknown"}
overrides = {}

with override_path.open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle, delimiter="\t")
    for row in reader:
        rel_path = (row.get("relative_path") or "").strip()
        bucket = (row.get("bucket") or "").strip()
        reason = (row.get("reason") or "manual_override").strip()
        if not rel_path:
            continue
        if not bucket:
            continue
        if bucket not in allowed:
            raise SystemExit(f"invalid override bucket for {rel_path}: {bucket}")
        overrides[rel_path] = {"bucket": bucket, "reason": reason}

rows = []
with classification_path.open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle, delimiter="\t")
    for row in reader:
        rel_path = row["relative_path"]
        if rel_path in overrides:
            row["bucket"] = overrides[rel_path]["bucket"]
            row["reason"] = overrides[rel_path]["reason"]
        rows.append(row)

output_path.parent.mkdir(parents=True, exist_ok=True)
with output_path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=["bucket", "reason", "relative_path"],
        delimiter="\t",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)

print(output_path)
PY

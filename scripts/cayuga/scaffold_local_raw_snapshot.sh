#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <local_raw_snapshot_root>" >&2
  exit 1
fi

RAW_ROOT="$1"

mkdir -p \
  "$RAW_ROOT/openalex" \
  "$RAW_ROOT/official_notices" \
  "$RAW_ROOT/pubmed"

cat > "$RAW_ROOT/README.txt" <<'EOF'
Expected local raw snapshot layout:

openalex/
official_notices/
pubmed/

Supported file types:
- openalex: .jsonl, .jsonl.gz, .gz
- official_notices: .jsonl, .jsonl.gz, .csv, .csv.gz
- pubmed: .jsonl, .jsonl.gz, .csv, .csv.gz, .xml, .xml.gz

Allowed sidecar files:
- source_versions.json
- sha256_manifest.tsv
- fetch.log
- openalex/manifest
- openalex/LICENSE.txt
- openalex/RELEASE_NOTES.txt

This directory is intended for local staging before:
1. ./scripts/cayuga/check_local_raw_snapshot.sh <local_raw_snapshot_root>
2. ./scripts/cayuga/sync_real_snapshot_to_cayuga.sh <remote_host> <run_root> <local_raw_snapshot_root>
EOF

echo "$RAW_ROOT"

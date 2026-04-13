#!/bin/bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <remote_host> <run_root> <local_raw_snapshot_root>" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"
LOCAL_RAW_ROOT="$3"
REMOTE_RAW_ROOT="$RUN_ROOT/raw/real_snapshot"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -d "$LOCAL_RAW_ROOT" ]; then
  echo "local raw snapshot root does not exist: $LOCAL_RAW_ROOT" >&2
  exit 1
fi

"$SCRIPT_DIR/check_local_raw_snapshot.sh" "$LOCAL_RAW_ROOT"

for name in openalex official_notices pubmed; do
  if [ ! -d "$LOCAL_RAW_ROOT/$name" ]; then
    echo "missing local raw subdirectory: $LOCAL_RAW_ROOT/$name" >&2
    exit 1
  fi
done

ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_RAW_ROOT/openalex' '$REMOTE_RAW_ROOT/official_notices' '$REMOTE_RAW_ROOT/pubmed'"

for name in openalex official_notices pubmed; do
  rsync -az "$LOCAL_RAW_ROOT/$name"/ "$REMOTE_HOST":"$REMOTE_RAW_ROOT/$name"/
done

echo "Synced raw snapshot to $REMOTE_HOST:$REMOTE_RAW_ROOT"

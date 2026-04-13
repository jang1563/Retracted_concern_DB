#!/bin/bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <remote_host> <run_root> <local_raw_snapshot_root>" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"
LOCAL_RAW_ROOT="$3"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"$SCRIPT_DIR/sync_real_snapshot_to_cayuga.sh" "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_RAW_ROOT"

ssh "$REMOTE_HOST" "cd '$RUN_ROOT/repo' && ./scripts/cayuga/check_real_snapshot_ready.sh '$RUN_ROOT' && ./scripts/cayuga/submit_real_ingest.sh '$RUN_ROOT'"

echo "Real ingest launched on $REMOTE_HOST:$RUN_ROOT"

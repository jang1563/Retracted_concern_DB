#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <remote_host> <run_root> [local_repo_root]" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"
LOCAL_REPO_ROOT="${3:-$(cd "$(dirname "$0")/../.." && pwd)}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"$SCRIPT_DIR/sync_repo_to_cayuga.sh" "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_REPO_ROOT"

ssh "$REMOTE_HOST" "cd '$RUN_ROOT/repo' && ./scripts/cayuga/setup_run_root.sh '$RUN_ROOT' && ./scripts/cayuga/verify_cayuga_runtime.sh '$RUN_ROOT' && ./scripts/cayuga/submit_overnight.sh '$RUN_ROOT'"

echo "Overnight rehearsal launched on $REMOTE_HOST:$RUN_ROOT"

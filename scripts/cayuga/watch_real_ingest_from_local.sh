#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <remote_host> <run_root> [poll_seconds] [max_polls]" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"
POLL_SECONDS="${3:-60}"
MAX_POLLS="${4:-0}"

ssh "$REMOTE_HOST" "cd '$RUN_ROOT/repo' && ./scripts/cayuga/watch_real_ingest.sh '$RUN_ROOT' '$POLL_SECONDS' '$MAX_POLLS'"

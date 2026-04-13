#!/bin/bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <remote_host> <run_root>" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"

ssh "$REMOTE_HOST" "cd '$RUN_ROOT/repo' && ./scripts/cayuga/report_public_vendor_storage.sh '$RUN_ROOT'"

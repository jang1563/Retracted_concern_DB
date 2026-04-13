#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <remote_host> <run_root> [local_repo_root]" >&2
  exit 1
fi

REMOTE_HOST="$1"
RUN_ROOT="$2"
LOCAL_REPO_ROOT="${3:-$(cd "$(dirname "$0")/../.." && pwd)}"
REMOTE_REPO_DIR="$RUN_ROOT/repo"

if [ ! -d "$LOCAL_REPO_ROOT" ]; then
  echo "local repo root does not exist: $LOCAL_REPO_ROOT" >&2
  exit 1
fi

ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_REPO_DIR'"

rsync -az \
  --exclude '.git/' \
  --exclude 'artifacts/' \
  --exclude 'data/normalized/' \
  --exclude 'data/raw/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  "$LOCAL_REPO_ROOT"/ "$REMOTE_HOST":"$REMOTE_REPO_DIR"/

echo "Synced repo to $REMOTE_HOST:$REMOTE_REPO_DIR"

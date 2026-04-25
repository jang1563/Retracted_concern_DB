#!/bin/bash
# Poll the remote open-data downstream job from the local machine until the
# release is ready, then let finalize_open_data_release_from_local.sh harvest
# artifacts and rebuild docs/results_v0.2.md.
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 6 ]; then
  echo "usage: $0 <remote_host> <remote_run_root> [poll_seconds] [max_polls] [local_dest_root] [output_doc]" >&2
  exit 1
fi

REMOTE_HOST="$1"
REMOTE_RUN_ROOT="$2"
POLL_SECONDS="${3:-60}"
MAX_POLLS="${4:-0}"
LOCAL_DEST_ROOT="${5:-}"
OUTPUT_DOC="${6:-}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FINALIZE_SCRIPT="${FINALIZE_SCRIPT:-$SCRIPT_DIR/finalize_open_data_release_from_local.sh}"

poll_count=0

while :; do
  poll_count=$((poll_count + 1))
  echo "poll=$poll_count generated_at=$(date '+%F %T')"

  set +e
  if [ -n "$LOCAL_DEST_ROOT" ] && [ -n "$OUTPUT_DOC" ]; then
    FINALIZE_OUTPUT="$("$FINALIZE_SCRIPT" "$REMOTE_HOST" "$REMOTE_RUN_ROOT" "$LOCAL_DEST_ROOT" "$OUTPUT_DOC" 2>&1)"
    FINALIZE_STATUS=$?
  elif [ -n "$LOCAL_DEST_ROOT" ]; then
    FINALIZE_OUTPUT="$("$FINALIZE_SCRIPT" "$REMOTE_HOST" "$REMOTE_RUN_ROOT" "$LOCAL_DEST_ROOT" 2>&1)"
    FINALIZE_STATUS=$?
  else
    FINALIZE_OUTPUT="$("$FINALIZE_SCRIPT" "$REMOTE_HOST" "$REMOTE_RUN_ROOT" 2>&1)"
    FINALIZE_STATUS=$?
  fi
  set -e

  printf "%s\n" "$FINALIZE_OUTPUT"

  if [ "$FINALIZE_STATUS" -ne 0 ]; then
    echo "watch_status=ERROR"
    exit "$FINALIZE_STATUS"
  fi

  if printf "%s\n" "$FINALIZE_OUTPUT" | grep -q '^release_ready=yes$'; then
    echo "watch_status=COMPLETED"
    exit 0
  fi

  if printf "%s\n" "$FINALIZE_OUTPUT" | grep -q '^next_action=resubmit_recommended$'; then
    echo "watch_status=RESUBMIT_RECOMMENDED"
    exit 1
  fi

  if printf "%s\n" "$FINALIZE_OUTPUT" | grep -q '^stale_progress=yes$'; then
    echo "watch_warning=STALE_PROGRESS"
  fi

  if [ "$MAX_POLLS" -gt 0 ] && [ "$poll_count" -ge "$MAX_POLLS" ]; then
    echo "max_polls_reached=$MAX_POLLS"
    echo "watch_status=TIMED_OUT"
    exit 0
  fi

  echo
  sleep "$POLL_SECONDS"
done

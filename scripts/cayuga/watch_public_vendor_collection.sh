#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <run_root> [poll_seconds] [max_polls]" >&2
  exit 1
fi

RUN_ROOT="$1"
POLL_SECONDS="${2:-60}"
MAX_POLLS="${3:-0}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ART_ROOT="$RUN_ROOT/artifacts/public_vendor_collection"
JOB_ID_FILE="$ART_ROOT/job_id.txt"
LOG_GLOB="$RUN_ROOT/logs/public-vendor-"*.out

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

if [ ! -f "$JOB_ID_FILE" ]; then
  echo "missing job id file: $JOB_ID_FILE" >&2
  exit 1
fi

JOB_ID="$(cat "$JOB_ID_FILE")"
poll_count=0

while :; do
  poll_count=$((poll_count + 1))
  echo "poll=$poll_count generated_at=$(date '+%F %T') job_id=$JOB_ID"

  if command -v squeue >/dev/null 2>&1; then
    squeue -j "$JOB_ID" -o "%i %j %T %M %R" || true
  fi

  if command -v sacct >/dev/null 2>&1; then
    sacct -j "$JOB_ID" --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS | sed -n '1,20p' || true
  fi

  if [ -f "$ART_ROOT/current_step.txt" ]; then
    echo "current_step=$(cat "$ART_ROOT/current_step.txt")"
  fi
  if [ -f "$ART_ROOT/failed_step.txt" ]; then
    echo "failed_step=$(cat "$ART_ROOT/failed_step.txt")"
  fi
  if [ -f "$ART_ROOT/summary.txt" ]; then
    echo "summary_file=$ART_ROOT/summary.txt"
    sed -n '1,40p' "$ART_ROOT/summary.txt" || true
  fi
  if [ -f "$ART_ROOT/FAILED" ]; then
    echo "public_vendor_collection_status=FAILED"
    exit 1
  fi
  if [ -f "$ART_ROOT/COMPLETED" ]; then
    echo "public_vendor_collection_status=COMPLETED"
    exit 0
  fi

  latest_log=""
  for log_path in $LOG_GLOB; do
    if [ -f "$log_path" ]; then
      latest_log="$log_path"
    fi
  done
  if [ -n "$latest_log" ]; then
    echo "latest_log=$latest_log"
    tail -n 20 "$latest_log" || true
  fi

  if [ "$MAX_POLLS" -gt 0 ] && [ "$poll_count" -ge "$MAX_POLLS" ]; then
    echo "max_polls_reached=$MAX_POLLS"
    exit 0
  fi

  echo
  sleep "$POLL_SECONDS"
done

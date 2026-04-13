#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
REPORT_PATH="$RUN_ROOT/artifacts/morning_status_report.txt"
PRE_JOB_FILE="$RUN_ROOT/artifacts/preflight/job_id.txt"
STRESS_JOB_FILE="$RUN_ROOT/artifacts/sample_stress/job_id.txt"
REAL_JOB_FILE="$RUN_ROOT/artifacts/real_release/job_id.txt"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

mkdir -p "$(dirname "$REPORT_PATH")"

{
  echo "run_root=$RUN_ROOT"
  echo "generated_at=$(date '+%F %T')"
  echo

  PRE_JOB_ID=""
  STRESS_JOB_ID=""
  REAL_JOB_ID=""

  if [ -f "$PRE_JOB_FILE" ]; then
    PRE_JOB_ID="$(cat "$PRE_JOB_FILE")"
    echo "preflight_job_id=$PRE_JOB_ID"
  else
    echo "preflight_job_id=missing"
  fi

  if [ -f "$STRESS_JOB_FILE" ]; then
    STRESS_JOB_ID="$(cat "$STRESS_JOB_FILE")"
    echo "sample_stress_job_id=$STRESS_JOB_ID"
  else
    echo "sample_stress_job_id=missing"
  fi

  if [ -f "$REAL_JOB_FILE" ]; then
    REAL_JOB_ID="$(cat "$REAL_JOB_FILE")"
    echo "real_ingest_job_id=$REAL_JOB_ID"
  else
    echo "real_ingest_job_id=missing"
  fi

  echo
  echo "[sentinels]"
  for path in \
    "$RUN_ROOT/artifacts/preflight/COMPLETED" \
    "$RUN_ROOT/artifacts/preflight/FAILED" \
    "$RUN_ROOT/artifacts/sample_stress/COMPLETED" \
    "$RUN_ROOT/artifacts/sample_stress/FAILED" \
    "$RUN_ROOT/artifacts/real_release/COMPLETED" \
    "$RUN_ROOT/artifacts/real_release/FAILED"
  do
    if [ -f "$path" ]; then
      echo "present	$path"
    fi
  done

  echo
  echo "[slurm]"
  JOB_IDS=()
  [ -n "$PRE_JOB_ID" ] && JOB_IDS+=("$PRE_JOB_ID")
  [ -n "$STRESS_JOB_ID" ] && JOB_IDS+=("$STRESS_JOB_ID")
  [ -n "$REAL_JOB_ID" ] && JOB_IDS+=("$REAL_JOB_ID")
  if ! command -v sacct >/dev/null 2>&1; then
    echo "sacct_not_available"
  elif [ "${#JOB_IDS[@]}" -gt 0 ]; then
    JOB_ID_LIST="$(IFS=,; echo "${JOB_IDS[*]}")"
    sacct -j "$JOB_ID_LIST" --format=JobID,JobName,State,Elapsed,MaxRSS
  else
    echo "no_job_ids_recorded"
  fi

  echo
  echo "[checksums]"
  CHECKSUMS="$RUN_ROOT/artifacts/sample_stress/checksums.tsv"
  if [ -f "$CHECKSUMS" ]; then
    LINE_COUNT="$(wc -l < "$CHECKSUMS" | tr -d ' ')"
    UNIQUE_COUNT="$(cut -f2-4 "$CHECKSUMS" | sort | uniq | wc -l | tr -d ' ')"
    echo "checksum_rows=$LINE_COUNT"
    echo "checksum_unique_tuples=$UNIQUE_COUNT"
  else
    echo "checksum_rows=missing"
  fi

  echo
  echo "[recent_logs]"
  for log_path in "$RUN_ROOT"/logs/*.out; do
    if [ ! -f "$log_path" ]; then
      continue
    fi
    echo "==> $log_path <=="
    tail -n 20 "$log_path"
    echo
  done
} | tee "$REPORT_PATH"

echo "report_path=$REPORT_PATH"

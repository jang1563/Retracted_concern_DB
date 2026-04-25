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
PUBLIC_JOB_FILE="$RUN_ROOT/artifacts/public_vendor_collection/job_id.txt"
OPEN_DATA_JOB_FILE="$RUN_ROOT/artifacts/open_data_release/job_id.txt"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

mkdir -p "$(dirname "$REPORT_PATH")"

artifact_status() {
  local art_root="$1"
  if [ -f "$art_root/FAILED" ]; then
    echo "FAILED"
  elif [ -f "$art_root/COMPLETED" ]; then
    echo "COMPLETED"
  elif [ -f "$art_root/current_step.txt" ]; then
    echo "IN_PROGRESS"
  elif [ -f "$art_root/job_id.txt" ]; then
    echo "JOB_RECORDED"
  else
    echo "NOT_SUBMITTED"
  fi
}

emit_artifact_report() {
  local label="$1"
  local art_root="$2"
  echo "artifact_status	$label	$(artifact_status "$art_root")"
  if [ -f "$art_root/current_step.txt" ]; then
    echo "current_step	$label	$(cat "$art_root/current_step.txt")"
  fi
  if [ -f "$art_root/failed_step.txt" ]; then
    echo "failed_step	$label	$(cat "$art_root/failed_step.txt")"
  fi
  if [ -f "$art_root/summary.txt" ]; then
    echo "summary_file	$label	$art_root/summary.txt"
  fi
  for path in "$art_root/COMPLETED" "$art_root/FAILED"; do
    if [ -f "$path" ]; then
      echo "present	$path"
    fi
  done
}

{
  echo "run_root=$RUN_ROOT"
  echo "generated_at=$(date '+%F %T')"
  echo

  PRE_JOB_ID=""
  STRESS_JOB_ID=""
  REAL_JOB_ID=""
  PUBLIC_JOB_ID=""
  OPEN_DATA_JOB_ID=""

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

  if [ -f "$PUBLIC_JOB_FILE" ]; then
    PUBLIC_JOB_ID="$(cat "$PUBLIC_JOB_FILE")"
    echo "public_vendor_collection_job_id=$PUBLIC_JOB_ID"
  else
    echo "public_vendor_collection_job_id=missing"
  fi

  if [ -f "$OPEN_DATA_JOB_FILE" ]; then
    OPEN_DATA_JOB_ID="$(cat "$OPEN_DATA_JOB_FILE")"
    echo "open_data_release_job_id=$OPEN_DATA_JOB_ID"
  else
    echo "open_data_release_job_id=missing"
  fi

  echo
  echo "[sentinels]"
  emit_artifact_report "preflight" "$RUN_ROOT/artifacts/preflight"
  emit_artifact_report "sample_stress" "$RUN_ROOT/artifacts/sample_stress"
  emit_artifact_report "real_release" "$RUN_ROOT/artifacts/real_release"
  emit_artifact_report "public_vendor_collection" "$RUN_ROOT/artifacts/public_vendor_collection"
  emit_artifact_report "open_data_release" "$RUN_ROOT/artifacts/open_data_release"

  echo
  echo "[slurm]"
  JOB_IDS=()
  [ -n "$PRE_JOB_ID" ] && JOB_IDS+=("$PRE_JOB_ID")
  [ -n "$STRESS_JOB_ID" ] && JOB_IDS+=("$STRESS_JOB_ID")
  [ -n "$REAL_JOB_ID" ] && JOB_IDS+=("$REAL_JOB_ID")
  [ -n "$PUBLIC_JOB_ID" ] && JOB_IDS+=("$PUBLIC_JOB_ID")
  [ -n "$OPEN_DATA_JOB_ID" ] && JOB_IDS+=("$OPEN_DATA_JOB_ID")
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

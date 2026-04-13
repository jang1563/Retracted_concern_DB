#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

if ! command -v scancel >/dev/null 2>&1; then
  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  source "$SCRIPT_DIR/common_job_env.sh"
  lsib_activate_slurm_client || true
  if ! command -v scancel >/dev/null 2>&1; then
    echo "scancel is not available in PATH" >&2
    exit 1
  fi
fi

RUN_ROOT="$1"
JOB_FILES=(
  "$RUN_ROOT/artifacts/preflight/job_id.txt"
  "$RUN_ROOT/artifacts/sample_stress/job_id.txt"
  "$RUN_ROOT/artifacts/real_release/job_id.txt"
)

JOB_IDS=()
for path in "${JOB_FILES[@]}"; do
  if [ -f "$path" ]; then
    JOB_IDS+=("$(cat "$path")")
  fi
done

if [ "${#JOB_IDS[@]}" -eq 0 ]; then
  echo "no recorded jobs found"
  exit 0
fi

scancel "${JOB_IDS[@]}"
printf "%s\n" "${JOB_IDS[@]}"

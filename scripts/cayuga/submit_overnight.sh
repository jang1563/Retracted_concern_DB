#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
JOB_DIR="$RUN_ROOT/jobs"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch is not available in PATH" >&2
  exit 1
fi

if [ ! -f "$JOB_DIR/preflight.sbatch" ]; then
  echo "missing $JOB_DIR/preflight.sbatch" >&2
  exit 1
fi

if [ ! -f "$JOB_DIR/sample_stress.sbatch" ]; then
  echo "missing $JOB_DIR/sample_stress.sbatch" >&2
  exit 1
fi

PRE_JOB_ID="$(sbatch "$JOB_DIR/preflight.sbatch" | awk '{print $NF}')"
STRESS_JOB_ID="$(sbatch --dependency=afterok:"$PRE_JOB_ID" "$JOB_DIR/sample_stress.sbatch" | awk '{print $NF}')"

printf "%s\n" "$PRE_JOB_ID" > "$RUN_ROOT/artifacts/preflight/job_id.txt"
printf "%s\n" "$STRESS_JOB_ID" > "$RUN_ROOT/artifacts/sample_stress/job_id.txt"

echo "preflight_job_id=$PRE_JOB_ID"
echo "sample_stress_job_id=$STRESS_JOB_ID"
echo "real_ingest_template is prepared but not submitted."

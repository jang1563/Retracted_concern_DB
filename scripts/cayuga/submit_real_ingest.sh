#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
JOB_DIR="$RUN_ROOT/jobs"
ART_ROOT="$RUN_ROOT/artifacts/real_release"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch is not available in PATH" >&2
  exit 1
fi

if [ ! -f "$JOB_DIR/real_ingest_template.sbatch" ]; then
  echo "missing $JOB_DIR/real_ingest_template.sbatch" >&2
  exit 1
fi

"$(cd "$(dirname "$0")" && pwd)/check_real_snapshot_ready.sh" "$RUN_ROOT"

mkdir -p "$ART_ROOT"
rm -f "$ART_ROOT/COMPLETED" "$ART_ROOT/FAILED" "$ART_ROOT/current_step.txt" "$ART_ROOT/failed_step.txt" "$ART_ROOT/job_id.txt"
JOB_ID="$(sbatch "$JOB_DIR/real_ingest_template.sbatch" | awk '{print $NF}')"
printf "%s\n" "$JOB_ID" > "$ART_ROOT/job_id.txt"

echo "real_ingest_job_id=$JOB_ID"

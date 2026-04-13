#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
JOB_DIR="$RUN_ROOT/jobs"
ART_ROOT="$RUN_ROOT/artifacts/public_vendor_collection"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$RUN_ROOT/repo"
TEMPLATE_PATH="$REPO_ROOT/scripts/cayuga/templates/public_vendor_collection.sbatch.in"
JOB_PATH="$JOB_DIR/public_vendor_collection.sbatch"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch is not available in PATH" >&2
  exit 1
fi

mkdir -p "$JOB_DIR"

if [ ! -f "$TEMPLATE_PATH" ]; then
  echo "missing $TEMPLATE_PATH" >&2
  exit 1
fi
sed "s|__RUN_ROOT__|$RUN_ROOT|g" "$TEMPLATE_PATH" > "$JOB_PATH"
chmod +x "$JOB_PATH"

JOB_ID="$(sbatch "$JOB_PATH" | awk '{print $NF}')"
mkdir -p "$ART_ROOT"
printf "%s\n" "$JOB_ID" > "$ART_ROOT/job_id.txt"

echo "public_vendor_collection_job_id=$JOB_ID"

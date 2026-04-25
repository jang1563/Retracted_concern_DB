#!/bin/bash
# Submit the downstream-only sbatch: register -> ingest -> materialize ->
# validate -> build-core -> build-splits -> audit-leakage -> train-a ->
# train-b -> build-site -> build-report, against an already-complete
# raw snapshot at $RUN_ROOT/raw/public_open_data_snapshot.
#
# Use this when the public-vendor-collection job timed out or was
# cancelled but the raw snapshot is intact, OR when you want to
# rebuild the release without re-running the collection phase.
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <run_root> [dependency_job_id]" >&2
  exit 1
fi

RUN_ROOT="$1"
DEPENDENCY_SPEC="${2:-}"
JOB_DIR="$RUN_ROOT/jobs"
ART_ROOT="$RUN_ROOT/artifacts/open_data_release"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$RUN_ROOT/repo"
TEMPLATE_PATH="$REPO_ROOT/scripts/cayuga/templates/open_data_downstream_only.sbatch.in"
JOB_PATH="$JOB_DIR/open_data_downstream_only.sbatch"

source "$SCRIPT_DIR/common_job_env.sh"
lsib_activate_slurm_client || true

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch is not available in PATH" >&2
  exit 1
fi

if [ ! -f "$TEMPLATE_PATH" ]; then
  echo "missing $TEMPLATE_PATH" >&2
  exit 1
fi

mkdir -p "$JOB_DIR" "$ART_ROOT"
sed "s|__RUN_ROOT__|$RUN_ROOT|g" "$TEMPLATE_PATH" > "$JOB_PATH"
chmod +x "$JOB_PATH"

if [ -z "$DEPENDENCY_SPEC" ]; then
  "$SCRIPT_DIR/check_local_raw_snapshot.sh" "$RUN_ROOT/raw/public_open_data_snapshot"
  rm -f "$ART_ROOT/COMPLETED" "$ART_ROOT/FAILED" "$ART_ROOT/current_step.txt" "$ART_ROOT/failed_step.txt" "$ART_ROOT/job_id.txt"
  JOB_ID="$(sbatch "$JOB_PATH" | awk '{print $NF}')"
else
  if [[ "$DEPENDENCY_SPEC" != *:* ]]; then
    DEPENDENCY_SPEC="afterok:$DEPENDENCY_SPEC"
  fi
  rm -f "$ART_ROOT/COMPLETED" "$ART_ROOT/FAILED" "$ART_ROOT/current_step.txt" "$ART_ROOT/failed_step.txt" "$ART_ROOT/job_id.txt"
  JOB_ID="$(sbatch --dependency="$DEPENDENCY_SPEC" "$JOB_PATH" | awk '{print $NF}')"
fi

printf "%s\n" "$JOB_ID" > "$ART_ROOT/job_id.txt"
echo "open_data_downstream_only_job_id=$JOB_ID"

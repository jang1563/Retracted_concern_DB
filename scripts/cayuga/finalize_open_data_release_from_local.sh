#!/bin/bash
# Check the remote open-data downstream job with a compatible Slurm client and,
# once complete, harvest the release locally and rebuild docs/results_v0.2.md
# plus the top-level README release snapshot.
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <remote_host> <remote_run_root> [local_dest_root] [output_doc]" >&2
  echo "  remote_host: e.g. cayuga-phobos" >&2
  echo "  remote_run_root: e.g. /athena/masonlab/scratch/users/jak4013/lsib/<run-id>" >&2
  echo "  local_dest_root: defaults to the local repo root" >&2
  echo "  output_doc: defaults to <repo_root>/docs/results_v0.2.md" >&2
  exit 1
fi

REMOTE_HOST="$1"
REMOTE_RUN_ROOT="$2"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOCAL_DEST_ROOT="${3:-$REPO_ROOT}"
OUTPUT_DOC="${4:-$REPO_ROOT/docs/results_v0.2.md}"
README_PATH="${README_PATH_OVERRIDE:-$REPO_ROOT/README.md}"
LOCAL_RELEASE_DIR="$LOCAL_DEST_ROOT/artifacts/open_data_release"
STEP_STALE_THRESHOLD_SECONDS="${STEP_STALE_THRESHOLD_SECONDS:-21600}"
LOG_STALE_THRESHOLD_SECONDS="${LOG_STALE_THRESHOLD_SECONDS:-21600}"
# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/common_python_env.sh"

SSH_BIN="${SSH_BIN:-ssh}"
HARVEST_SCRIPT="${HARVEST_SCRIPT:-$REPO_ROOT/scripts/cayuga/harvest_open_data_release_to_local.sh}"
lsib_require_python_bin "${PYTHON_BIN:-}"

STATUS_OUTPUT="$(
  "$SSH_BIN" "$REMOTE_HOST" /bin/bash -s -- "$REMOTE_RUN_ROOT" <<'EOF'
set -euo pipefail

RUN_ROOT="$1"
ART_ROOT="$RUN_ROOT/artifacts/open_data_release"
JOB_FILE="$ART_ROOT/job_id.txt"
JOB_ID="unknown"
SLURM_BIN=""
JOB_ELAPSED=""
JOB_TIME_LIMIT=""
JOB_START=""
JOB_NODE=""
SSTAT_AVE_CPU=""
SSTAT_MAX_RSS=""
SSTAT_AVE_DISK_READ=""
SSTAT_AVE_DISK_WRITE=""
NOW_EPOCH="$(date +%s)"

if [ -f "$JOB_FILE" ]; then
  JOB_ID="$(cat "$JOB_FILE")"
fi

choose_slurm_bin() {
  local candidate
  for candidate in \
    /opt/ohpc/pub/software/slurm/23.11.11/bin \
    /opt/ohpc/pub/software/slurm/24.05.2/bin \
    /opt/ohpc/pub/software/slurm/25.05.0/bin \
    /usr/bin
  do
    if [ -x "$candidate/sacct" ] && [ -x "$candidate/squeue" ]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

STATUS="UNKNOWN"
if [ -f "$ART_ROOT/FAILED" ]; then
  STATUS="FAILED_MARKER"
elif [ -f "$ART_ROOT/COMPLETED" ]; then
  STATUS="COMPLETED"
else
  SLURM_BIN="$(choose_slurm_bin || true)"
  if [ -n "$SLURM_BIN" ] && [ "$JOB_ID" != "unknown" ]; then
    STATUS="$("$SLURM_BIN/sacct" -j "$JOB_ID" --format=State -n -P 2>/dev/null | awk 'NF {print $1; exit}')"
    SQUEUE_LINE="$("$SLURM_BIN/squeue" -j "$JOB_ID" -h -o "%T|%M|%l|%S|%R" 2>/dev/null | awk 'NF {print $0; exit}')"
    if [ -n "$SQUEUE_LINE" ]; then
      IFS='|' read -r QUEUE_STATUS JOB_ELAPSED JOB_TIME_LIMIT JOB_START JOB_NODE <<<"$SQUEUE_LINE"
      if [ -z "$STATUS" ]; then
        STATUS="$QUEUE_STATUS"
      fi
    fi
    if [ -x "$SLURM_BIN/sstat" ]; then
      SSTAT_LINE="$("$SLURM_BIN/sstat" -j "${JOB_ID}.batch" --format=AveCPU,MaxRSS,AveDiskRead,AveDiskWrite -P -n 2>/dev/null | awk 'NF {print $0; exit}')"
      if [ -n "$SSTAT_LINE" ]; then
        IFS='|' read -r SSTAT_AVE_CPU SSTAT_MAX_RSS SSTAT_AVE_DISK_READ SSTAT_AVE_DISK_WRITE <<<"$SSTAT_LINE"
      fi
    fi
  fi
  STATUS="${STATUS:-UNKNOWN}"
fi

echo "job_id=$JOB_ID"
echo "job_id_file=$JOB_FILE"
echo "job_status=$STATUS"
if [ -n "$JOB_ELAPSED" ]; then
  echo "job_elapsed=$JOB_ELAPSED"
fi
if [ -n "$JOB_TIME_LIMIT" ]; then
  echo "job_time_limit=$JOB_TIME_LIMIT"
fi
if [ -n "$JOB_START" ]; then
  echo "job_start=$JOB_START"
fi
if [ -n "$JOB_NODE" ]; then
  echo "job_node=$JOB_NODE"
fi
if [ -n "$SSTAT_AVE_CPU" ] || [ -n "$SSTAT_MAX_RSS" ] || [ -n "$SSTAT_AVE_DISK_READ" ] || [ -n "$SSTAT_AVE_DISK_WRITE" ]; then
  echo "resource_activity=observed"
  echo "sstat_ave_cpu=$SSTAT_AVE_CPU"
  echo "sstat_max_rss=$SSTAT_MAX_RSS"
  echo "sstat_ave_disk_read=$SSTAT_AVE_DISK_READ"
  echo "sstat_ave_disk_write=$SSTAT_AVE_DISK_WRITE"
elif [ "$STATUS" != "COMPLETED" ]; then
  echo "resource_activity=unknown"
fi

if [ -f "$ART_ROOT/current_step.txt" ]; then
  echo "current_step=$(cat "$ART_ROOT/current_step.txt")"
  echo "current_step_mtime=$(stat -c '%y' "$ART_ROOT/current_step.txt" 2>/dev/null || date -r "$ART_ROOT/current_step.txt" '+%F %T')"
  STEP_EPOCH="$(stat -c '%Y' "$ART_ROOT/current_step.txt" 2>/dev/null || true)"
  if [ -n "$STEP_EPOCH" ] && [ "$STEP_EPOCH" -le "$NOW_EPOCH" ] 2>/dev/null; then
    echo "current_step_age_seconds=$((NOW_EPOCH - STEP_EPOCH))"
  fi
fi
if [ -f "$ART_ROOT/failed_step.txt" ]; then
  echo "failed_step=$(cat "$ART_ROOT/failed_step.txt")"
fi

LOG_PATH="$RUN_ROOT/logs/open-data-downstream-${JOB_ID}.out"
if [ -f "$LOG_PATH" ]; then
  echo "log_path=$LOG_PATH"
  echo "log_mtime=$(stat -c '%y' "$LOG_PATH" 2>/dev/null || date -r "$LOG_PATH" '+%F %T')"
  LOG_EPOCH="$(stat -c '%Y' "$LOG_PATH" 2>/dev/null || true)"
  if [ -n "$LOG_EPOCH" ] && [ "$LOG_EPOCH" -le "$NOW_EPOCH" ] 2>/dev/null; then
    echo "log_age_seconds=$((NOW_EPOCH - LOG_EPOCH))"
  fi
  tail -n 3 "$LOG_PATH" | sed 's/^/log_tail=/'
fi
EOF
)"

printf "%s\n" "$STATUS_OUTPUT"

JOB_STATUS="$(printf "%s\n" "$STATUS_OUTPUT" | awk -F= '/^job_status=/{print $2; exit}')"
RESOURCE_ACTIVITY="$(printf "%s\n" "$STATUS_OUTPUT" | awk -F= '/^resource_activity=/{print $2; exit}')"
STEP_AGE_SECONDS="$(printf "%s\n" "$STATUS_OUTPUT" | awk -F= '/^current_step_age_seconds=/{print $2; exit}')"
LOG_AGE_SECONDS="$(printf "%s\n" "$STATUS_OUTPUT" | awk -F= '/^log_age_seconds=/{print $2; exit}')"

STALE_PROGRESS="no"
if [ "$JOB_STATUS" != "COMPLETED" ]; then
  if [ -n "$STEP_AGE_SECONDS" ] && [ "$STEP_AGE_SECONDS" -ge "$STEP_STALE_THRESHOLD_SECONDS" ]; then
    echo "current_step_stale=yes"
    STALE_PROGRESS="yes"
  fi
  if [ -n "$LOG_AGE_SECONDS" ] && [ "$LOG_AGE_SECONDS" -ge "$LOG_STALE_THRESHOLD_SECONDS" ]; then
    echo "log_stale=yes"
    STALE_PROGRESS="yes"
  fi
  if [ "$STALE_PROGRESS" = "yes" ]; then
    echo "stale_progress=yes"
    echo "step_stale_threshold_seconds=$STEP_STALE_THRESHOLD_SECONDS"
    echo "log_stale_threshold_seconds=$LOG_STALE_THRESHOLD_SECONDS"
    if [ "$RESOURCE_ACTIVITY" = "observed" ]; then
      echo "stale_progress_context=resource_activity_observed"
      echo "progress_recommendation=inspect_runtime_or_wait"
    else
      echo "stale_progress_context=resource_activity_unknown"
      echo "progress_recommendation=investigate_before_resubmit"
    fi
  fi
fi

if [ "$JOB_STATUS" != "COMPLETED" ]; then
  echo "release_ready=no"
  case "$JOB_STATUS" in
    FAILED*|CANCELLED*|TIMEOUT*|NODE_FAIL*)
      echo "next_action=resubmit_recommended"
      ;;
  esac
  exit 0
fi

echo "release_ready=yes"
"$HARVEST_SCRIPT" "$REMOTE_HOST" "$REMOTE_RUN_ROOT" "$LOCAL_DEST_ROOT"
PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
  "$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --root-dir "$REPO_ROOT" \
  --release-dir "$LOCAL_RELEASE_DIR" \
  build-results-v0-2 \
  --output-path "$OUTPUT_DOC" \
  --run-root "$REMOTE_RUN_ROOT"

PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
  "$PYTHON_BIN" -m life_science_integrity_benchmark.cli \
  --root-dir "$REPO_ROOT" \
  --release-dir "$LOCAL_RELEASE_DIR" \
  build-readme-v0-2 \
  --output-path "$README_PATH" \
  --results-doc-path "docs/results_v0.2.md"

echo "results_doc=$OUTPUT_DOC"
echo "readme_doc=$README_PATH"

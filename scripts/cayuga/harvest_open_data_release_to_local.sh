#!/bin/bash
# Harvest a completed open-data release from Cayuga into the local repo.
#
# Preconditions:
#   - Downstream-only job has written $RUN_ROOT/artifacts/open_data_release/COMPLETED
#   - SSH to the remote host works (use cayuga-phobos if cayuga-login1 is flaky)
#
# What it does:
#   1. Verifies COMPLETED marker on the remote
#   2. rsyncs artifacts/open_data_release/ and artifacts/open_data_site/ to
#      local paths under this repo (gitignored)
#   3. Prints a human-readable summary of record counts, leakage status,
#      and baseline metrics so the numbers can be pasted into
#      docs/results_v0.2.md
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <remote_host> <remote_run_root> [local_dest_root]" >&2
  echo "  remote_host: e.g. cayuga-phobos" >&2
  echo "  remote_run_root: e.g. /athena/masonlab/scratch/users/jak4013/lsib/<run-id>" >&2
  echo "  local_dest_root: default is the repo root; release lands at" >&2
  echo "                   \$local_dest_root/artifacts/open_data_release/" >&2
  exit 1
fi

REMOTE_HOST="$1"
REMOTE_RUN_ROOT="$2"
LOCAL_DEST_ROOT="${3:-$(cd "$(dirname "$0")/../.." && pwd)}"

REMOTE_RELEASE="$REMOTE_RUN_ROOT/artifacts/open_data_release"
REMOTE_SITE="$REMOTE_RUN_ROOT/artifacts/open_data_site"
LOCAL_RELEASE="$LOCAL_DEST_ROOT/artifacts/open_data_release"
LOCAL_SITE="$LOCAL_DEST_ROOT/artifacts/open_data_site"

echo "== Checking remote completion marker =="
if ! ssh -o ConnectTimeout=30 "$REMOTE_HOST" "test -f '$REMOTE_RELEASE/COMPLETED'"; then
  echo "COMPLETED marker not found at $REMOTE_HOST:$REMOTE_RELEASE/COMPLETED" >&2
  echo "current_step on remote:" >&2
  ssh -o ConnectTimeout=30 "$REMOTE_HOST" "cat '$REMOTE_RELEASE/current_step.txt' 2>/dev/null || echo '(no current_step.txt)'" >&2
  echo "Aborting harvest — the job has not yet finished." >&2
  exit 1
fi
echo "ok"

mkdir -p "$LOCAL_RELEASE" "$LOCAL_SITE"

echo "== rsync release =="
rsync -az --delete "$REMOTE_HOST:$REMOTE_RELEASE/" "$LOCAL_RELEASE/"
echo "== rsync site =="
rsync -az --delete "$REMOTE_HOST:$REMOTE_SITE/" "$LOCAL_SITE/"

echo
echo "== Release summary =="
python3 - <<PY
import json, sys
from pathlib import Path

release = Path("$LOCAL_RELEASE")

def load(name):
    p = release / name
    if not p.exists():
        print(f"  (missing: {name})")
        return None
    try:
        return json.loads(p.read_text())
    except Exception as exc:
        print(f"  (unreadable: {name}: {exc})")
        return None

summary = load("summary.json")
leakage = load("leakage_report.json")
task_a = load("task_a_baselines.json")
task_b = load("task_b_baseline.json")

if summary:
    print("Snapshot date:        ", summary.get("snapshot_date"))
    print("Record count:         ", summary.get("record_count"))
    print("Auto-publish records: ", summary.get("auto_publish_count"))
    print("Curator-review:       ", summary.get("curated_review_count"))
    print("Task A 12m eligible:  ", summary.get("task_a_12m_eligible_count"))
    print("Task A 36m eligible:  ", summary.get("task_a_36m_eligible_count"))
    print("Noisy-date excluded:  ", summary.get("task_a_noisy_date_count"))
    print("Notice status:")
    for k, v in sorted((summary.get("notice_status_counts") or {}).items()):
        print(f"  {k:30s} {v}")
    print("Subfield counts:")
    for k, v in sorted((summary.get("subfield_counts") or {}).items()):
        print(f"  {k:30s} {v}")
    print()

if leakage:
    status = "PASS" if leakage.get("passed") else "FAIL"
    print(f"Leakage audit:        {status}  (records={leakage.get('records_checked')})")
    viol = sum(len(leakage.get(k, [])) for k in (
        "feature_cutoff_violations",
        "leaked_fields_found",
        "records_missing_feature_provenance",
        "records_with_invalid_event_order",
        "records_with_snapshot_violations",
    ))
    print(f"Total violations:     {viol}")
    print()

def dump_task_a(horizon):
    runs = (task_a or {}).get(horizon) or []
    if not runs:
        return
    print(f"Task A {horizon}:")
    for r in runs:
        m = r.get("metrics", {})
        print(f"  {r.get('model_name'):32s} AUPRC={m.get('AUPRC'):.4g}  R@1%={m.get('Recall@1pct'):.3g}  R@5%={m.get('Recall@5pct'):.3g}  ECE={m.get('ECE'):.3g}")

dump_task_a("task_a_12m")
dump_task_a("task_a_36m")

if task_b:
    m = task_b.get("metrics", {})
    print(f"Task B:                 acc={m.get('notice_status_accuracy'):.3g}  macroF1={m.get('tag_macro_f1'):.3g}  prov_cov={m.get('provenance_coverage'):.3g}")

print()
print("Files:")
for p in sorted(release.glob("*")):
    print(f"  {p.name}")
PY

echo
echo "Local release:  $LOCAL_RELEASE"
echo "Local site:     $LOCAL_SITE"
echo "Paste the numbers above into docs/results_v0.2.md and open artifacts/open_data_site/index.html to review the evidence browser."

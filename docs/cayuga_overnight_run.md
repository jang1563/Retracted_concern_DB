# HPC Overnight Run

This document is the operational guide that turns the HPC overnight run plan into an executable workflow. The nightly path is an HPC rehearsal only; the real-data job is rendered as a template and not submitted.

## Fixed choices

- Run root: `<hpc-scratch-root>/lsib/<run-id>` (e.g. `/scratch/users/<user>/lsib/20260410-overnight`)
- Mode: `sbatch`
- Env: `python.3.12.R.4.3.3`
- Backend: `hashing`
- No GPU, no `transformers`, no Hugging Face cache, no XGBoost

## Files added for this workflow

- `scripts/cayuga/sync_repo_to_cayuga.sh`
- `scripts/cayuga/scaffold_local_raw_snapshot.sh`
- `scripts/cayuga/check_local_raw_snapshot.sh`
- `scripts/cayuga/inventory_local_raw_snapshot.sh`
- `scripts/cayuga/classify_mixed_sources.sh`
- `scripts/cayuga/review_unknown_classification.sh`
- `scripts/cayuga/write_unknown_override_template.sh`
- `scripts/cayuga/apply_classification_overrides.sh`
- `scripts/cayuga/stage_mixed_sources_into_raw_snapshot.sh`
- `scripts/cayuga/run_local_real_snapshot_pipeline.sh`
- `scripts/cayuga/sync_real_snapshot_to_cayuga.sh`
- `scripts/cayuga/setup_run_root.sh`
- `scripts/cayuga/verify_cayuga_runtime.sh`
- `scripts/cayuga/submit_overnight.sh`
- `scripts/cayuga/check_real_snapshot_ready.sh`
- `scripts/cayuga/submit_real_ingest.sh`
- `scripts/cayuga/collect_morning_status.sh`
- `scripts/cayuga/launch_overnight_from_local.sh`
- `scripts/cayuga/launch_real_ingest_from_local.sh`
- `scripts/cayuga/watch_real_ingest.sh`
- `scripts/cayuga/watch_real_ingest_from_local.sh`
- `scripts/cayuga/cancel_recorded_jobs.sh`
- `scripts/cayuga/common_job_env.sh`
- `scripts/cayuga/templates/preflight.sbatch.in`
- `scripts/cayuga/templates/sample_stress.sbatch.in`
- `scripts/cayuga/templates/real_ingest_template.sbatch.in`

## Tonight sequence

### 1. Sync the repo from the local machine

```bash
REMOTE_HOST="<user>@<hpc-login>"
RUN_ROOT="<hpc-scratch-root>/lsib/<run-id>"

./scripts/cayuga/sync_repo_to_cayuga.sh "$REMOTE_HOST" "$RUN_ROOT"
```

To run sync-through-submit in one shot:

```bash
./scripts/cayuga/launch_overnight_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
```

### 2. SSH to the HPC login and render the run root

```bash
ssh "$REMOTE_HOST"
cd "$RUN_ROOT/repo"
./scripts/cayuga/setup_run_root.sh "$RUN_ROOT"
./scripts/cayuga/verify_cayuga_runtime.sh "$RUN_ROOT"
```

After this step, the following are ready:

- `jobs/preflight.sbatch`
- `jobs/sample_stress.sbatch`
- `jobs/real_ingest_template.sbatch`
- `raw/real_snapshot/README.txt`

### 3. Submit only the rehearsal jobs

```bash
./scripts/cayuga/submit_overnight.sh "$RUN_ROOT"
```

This script submits only:

- `preflight.sbatch`
- `sample_stress.sbatch` with `afterok:<preflight_jobid>`

`real_ingest_template.sbatch` is not submitted.

### 4. Morning summary in one command

```bash
./scripts/cayuga/collect_morning_status.sh "$RUN_ROOT"
```

This script bundles job ids, per-artifact effective status, `sacct` output,
checksum uniqueness, and a tail of recent logs into
`artifacts/morning_status_report.txt`. Its `artifact_status` lines cover
`preflight`, `sample_stress`, `real_release`, `public_vendor_collection`, and
`open_data_release`, with `FAILED` taking precedence over `COMPLETED` so stale
success markers do not hide rerun failures.

## What each job does

### `preflight.sbatch`

- `python3 -m unittest discover -s tests -v`
- `python3 -m life_science_integrity_benchmark.cli ... demo`
- success sentinel: `artifacts/preflight/COMPLETED`
- failure sentinel: `artifacts/preflight/FAILED`

### `sample_stress.sbatch`

- one workdir reused: `artifacts/sample_stress/work/`
- loops `demo` until the 8-hour budget is nearly exhausted
- writes `sha256` tuples to `artifacts/sample_stress/checksums.tsv`
- stops on first failure
- success sentinel: `artifacts/sample_stress/COMPLETED`
- failure sentinel: `artifacts/sample_stress/FAILED`

### `real_ingest_template.sbatch`

- prepared only
- submit on a later night after raw files are dropped into:
  - `raw/real_snapshot/openalex/`
  - `raw/real_snapshot/official_notices/`
  - `raw/real_snapshot/pubmed/`

## Morning checks

```bash
RUN_ROOT="<hpc-scratch-root>/lsib/<run-id>"

./scripts/cayuga/collect_morning_status.sh "$RUN_ROOT"

sacct -j "$(cat "$RUN_ROOT/artifacts/preflight/job_id.txt")","$(cat "$RUN_ROOT/artifacts/sample_stress/job_id.txt")" \
  --format=JobID,JobName,State,Elapsed,MaxRSS

grep '^artifact_status' "$RUN_ROOT/artifacts/morning_status_report.txt"
grep '^current_step' "$RUN_ROOT/artifacts/morning_status_report.txt"
wc -l "$RUN_ROOT/artifacts/sample_stress/checksums.tsv"
cut -f2-4 "$RUN_ROOT/artifacts/sample_stress/checksums.tsv" | sort | uniq | wc -l
```

Success means:

- `artifact_status	preflight	COMPLETED`
- `artifact_status	sample_stress	COMPLETED`
- `checksums.tsv` has at least 2 rows
- checksum tuple unique count is `1`
- no artifact that matters is reporting `FAILED`

## Real-data readiness

The next real-data run is ready when:

- local raw snapshot has been scaffolded and validated
- `raw/real_snapshot/openalex/` contains real OpenAlex shards
- `raw/real_snapshot/official_notices/` contains local notice exports
- `raw/real_snapshot/pubmed/` contains DOI-joinable PubMed files
- `jobs/real_ingest_template.sbatch` exists

At that point the next action is to submit only:

```bash
LOCAL_RAW_ROOT="/path/to/local/raw_snapshot"

./scripts/cayuga/scaffold_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/check_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/inventory_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/run_local_real_snapshot_pipeline.sh "$LOCAL_RAW_ROOT" /tmp/lsib-real-dryrun
./scripts/cayuga/sync_real_snapshot_to_cayuga.sh "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_RAW_ROOT"
./scripts/cayuga/check_real_snapshot_ready.sh "$RUN_ROOT"
./scripts/cayuga/submit_real_ingest.sh "$RUN_ROOT"
```

The dry-run script writes ingest manifests and normalized shards under `/tmp/lsib-real-dryrun/runtime_root`, so the repo checkout's `data/` is left untouched.

If your downloads folder has mixed sources, classify and stage them first:

```bash
MIXED_SOURCE_ROOT="/path/to/downloads_or_exports"
./scripts/cayuga/classify_mixed_sources.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/review_unknown_classification.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/write_unknown_override_template.sh "$MIXED_SOURCE_ROOT/source_classification.tsv"
./scripts/cayuga/stage_mixed_sources_into_raw_snapshot.sh "$MIXED_SOURCE_ROOT" "$LOCAL_RAW_ROOT" copy "$MIXED_SOURCE_ROOT/source_classification.tsv" "$MIXED_SOURCE_ROOT/classification_overrides.tsv"
```

To do the same work in a single command from the local machine:

```bash
./scripts/cayuga/launch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_RAW_ROOT"
```

To watch real-ingest status from the local machine:

```bash
./scripts/cayuga/watch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 20
```

Current submit wrappers also clear stale sentinel files immediately after a new
`sbatch` succeeds, so reruns start with a clean marker state.

For the open-data downstream release path used by the current v0.2 build:

```bash
./scripts/cayuga/finalize_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
./scripts/cayuga/watch_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 300
```

These local wrappers are the recommended way to monitor or finalize the release
because they select a compatible Cayuga Slurm client before calling `squeue` or
`sacct`. On `cayuga-phobos`, the default `squeue` in `PATH` may be too new and
return an incompatible-protocol error even while the job is healthy.

To watch public-source collection status from the local machine:

```bash
./scripts/cayuga/watch_public_vendor_collection_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 10
```

To cancel recorded jobs:

```bash
./scripts/cayuga/cancel_recorded_jobs.sh "$RUN_ROOT"
```

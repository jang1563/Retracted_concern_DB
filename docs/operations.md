# Operations Runbook

This file is the entry point for the operational / HPC side of the project. The top-level `README.md` covers the 60-second demo and the benchmark design; everything below is for operators running real-data collection, ingest, and release builds.

## Real-Data Collection And Ingest

### Monthly open-data freeze (no paid vendors)

```bash
cp env/raw_snapshot.env.example /tmp/raw_snapshot.env
$EDITOR /tmp/raw_snapshot.env
export RAW_SNAPSHOT_ENV_FILE="/tmp/raw_snapshot.env"
export CROSSREF_SOURCE_MODE="skip"

./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_open_data_freeze.sh \
  /data/vendor_archive /data/raw_snapshot /tmp/lsib-freeze-run 2026-03-freeze
```

OpenAlex collection records the freeze end date and prunes any `updated_date=YYYY-MM-DD` works partitions after that date before writing the vendor manifest, so reruns do not silently fall forward into later monthly data. PubMed collection similarly keeps only same-year baseline files and updatefiles whose directory-listing modified date is at or before the freeze end.

### Monthly freeze with Crossref Metadata Plus

```bash
./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_monthly_freeze.sh \
  /data/vendor_archive /data/raw_snapshot /tmp/lsib-freeze-run 2026-03-freeze
```

### Validate and stage an existing vendor archive

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  validate-vendor-archive --vendor-root /data/vendor_archive --snapshot-label 2026-03-freeze

PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  stage-vendor-archive --vendor-root /data/vendor_archive \
  --raw-root /data/raw_snapshot --snapshot-label 2026-03-freeze
```

OpenAlex, PubMed, and Retraction Watch are required for staging; Crossref Metadata Plus is optional only when `--allow-missing-crossref` is passed for an open-data-only run.

Or in one shot:

```bash
./scripts/raw_snapshot/run_vendor_archive_pipeline.sh \
  /data/vendor_archive /data/raw_snapshot /tmp/lsib-vendor-dryrun 2026-03-freeze
```

## Register, Ingest, And Materialize

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  scaffold-real-ingest --raw-dir data/raw/my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  register-snapshot --snapshot-id my_snapshot --raw-root data/raw/my_snapshot \
  --source-family openalex_notices
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  ingest-snapshot --snapshot-id my_snapshot --collector openalex_bulk
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  ingest-snapshot --snapshot-id my_snapshot --collector local_notice_export
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  ingest-snapshot --snapshot-id my_snapshot --collector pubmed_index
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  materialize-canonical --snapshot-id my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  validate-snapshot --snapshot-id my_snapshot --release-dir artifacts/real_release
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  --source-dir data/normalized/my_snapshot/canonical \
  --release-dir artifacts/real_release \
  --site-dir artifacts/real_site \
  build-core
```

If the ingest registry and normalized shards should live outside the repo checkout, pass `--root-dir`:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  --root-dir /tmp/lsib-runtime \
  register-snapshot --snapshot-id my_snapshot --raw-root /tmp/raw_snapshot \
  --source-family openalex_notices
```

The legacy compatibility wrapper for flat `articles.jsonl` inputs:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli \
  normalize-real-sources --raw-dir data/raw_real --normalized-dir data/sources_real
```

## HPC (Cayuga) Workflows

Detailed cluster runbook: [docs/cayuga_overnight_run.md](cayuga_overnight_run.md).

Typical overnight rehearsal sequence:

```bash
REMOTE_HOST="<user>@<hpc-login>"
RUN_ROOT="<hpc-scratch-root>/lsib/<run-id>"

./scripts/cayuga/sync_repo_to_cayuga.sh "$REMOTE_HOST" "$RUN_ROOT"
ssh "$REMOTE_HOST"
cd "$RUN_ROOT/repo"
./scripts/cayuga/setup_run_root.sh "$RUN_ROOT"
./scripts/cayuga/submit_overnight.sh "$RUN_ROOT"
```

Or from local in one step:

```bash
./scripts/cayuga/launch_overnight_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
```

Real ingest (after raw snapshot is ready locally):

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

Classify mixed-source downloads into the raw snapshot layout:

```bash
MIXED_SOURCE_ROOT="/path/to/downloads_or_exports"
./scripts/cayuga/classify_mixed_sources.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/review_unknown_classification.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/write_unknown_override_template.sh "$MIXED_SOURCE_ROOT/source_classification.tsv"
./scripts/cayuga/stage_mixed_sources_into_raw_snapshot.sh \
  "$MIXED_SOURCE_ROOT" "$LOCAL_RAW_ROOT" copy \
  "$MIXED_SOURCE_ROOT/source_classification.tsv" \
  "$MIXED_SOURCE_ROOT/classification_overrides.tsv"
```

## Monitoring A Running HPC Job

Watch a submitted real-ingest job from the local machine:

```bash
./scripts/cayuga/watch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 20
```

For the current open-data downstream release path (the v0.2 real-data build),
use the local wrappers that query Cayuga with a compatible Slurm client and
then harvest/rebuild the results doc when the release is ready:

```bash
./scripts/cayuga/finalize_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
./scripts/cayuga/watch_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 300
```

On `cayuga-phobos`, the default `squeue` in `PATH` may point to a newer Slurm
client that reports an incompatible-protocol error. The helpers above avoid
that footgun by explicitly selecting a compatible Cayuga Slurm install before
querying job state.

If the downstream job stays in one stage for a long time, the finalize/watch
helpers now surface `current_step_age_seconds`, `log_age_seconds`, and a
derived `stale_progress=yes` warning once those ages exceed 6 hours by default.
This does not mean the job is dead by itself: if `resource_activity=observed`
also appears, the more accurate interpretation is "quiet but still consuming
CPU / I/O". Override the warning threshold with
`STEP_STALE_THRESHOLD_SECONDS=<seconds>` and
`LOG_STALE_THRESHOLD_SECONDS=<seconds>` if needed.

On reruns that use the current sbatch templates, `ingest-snapshot` also updates
`current_step.txt` from inside the Python process, so the file can advance from
plain step names like `ingest_openalex` to heartbeat-style payloads such as
`ingest_openalex 17/2127 raw_records=...`. That makes the stale-progress
warning substantially more trustworthy for long OpenAlex ingests.

Morning status summary:

```bash
./scripts/cayuga/collect_morning_status.sh "$RUN_ROOT"
```

That report now emits an `artifact_status` line for each major artifact root
(`preflight`, `sample_stress`, `real_release`, `public_vendor_collection`,
`open_data_release`). The status is resolved with `FAILED` taking precedence
over `COMPLETED`, then `IN_PROGRESS`, then `JOB_RECORDED`, which makes reruns
easier to triage when old marker files might otherwise be misleading.

Cancel recorded Cayuga jobs from the cluster:

```bash
./scripts/cayuga/cancel_recorded_jobs.sh "$RUN_ROOT"
```

## Resuming A Timed-Out Or Cancelled Run

The default `public_vendor_collection.sbatch.in` template removes `$WORK_ROOT` on entry and sets `--time=24:00:00`. That combination makes a naive resubmission destructive, and 24h is often too short for a full ingest + downstream build against a real-data snapshot.

The recommended resume path is to skip the collection phase entirely once the raw snapshot itself is complete (three non-empty buckets: `openalex/`, `official_notices/`, `pubmed/` under `$RUN_ROOT/raw/public_open_data_snapshot`). In that case, submit the downstream-only job:

```bash
ssh "$REMOTE_HOST"
cd "$RUN_ROOT/repo"
./scripts/cayuga/submit_open_data_downstream_only.sh "$RUN_ROOT"
```

From the local machine, the easiest monitor/finalize sequence after submission is:

```bash
./scripts/cayuga/watch_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 300
```

Or, if you only want a one-shot status check that harvests immediately when the
release completes:

```bash
./scripts/cayuga/finalize_open_data_release_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
```

What it does:

- `#SBATCH --time=5-00:00:00` so ingest + materialize + build-core + splits + audit + training + site + report fit inside one job.
- Does not wipe `$WORK_ROOT` or any existing runtime root on entry.
- Does not gate on the public-vendor-collection `COMPLETED` marker, because the raw snapshot being complete is the real precondition.
- Uses a separate node-local runtime root (`$SLURM_TMPDIR` when available, otherwise `$TMPDIR`/`/tmp`) instead of the stale `public_vendor_collection_work/runtime_root/`, which keeps the ingest manifest clean and reduces exposure to shared scratch I/O failures.
- Writes release artifacts to `artifacts/open_data_release/` and the site to `artifacts/open_data_site/`.
- Current submit helpers clear stale `COMPLETED` / `FAILED` / step-marker files immediately after a new `sbatch` succeeds, and the watch helpers treat `FAILED` as higher priority than `COMPLETED` if both exist. That prevents reruns from inheriting an old success signal.

The `scu-cpu` partition on Cayuga permits up to 7-day walltimes; 5 days is a conservative default. The original 24h in `public_vendor_collection.sbatch.in` and `open_data_finalize_template.sbatch.in` is a template choice, not a cluster ceiling.

### When to patch the public-vendor-collection template instead

If you need to re-run collection (for example, the raw snapshot itself is incomplete), copy `public_vendor_collection.sbatch.in`, remove the `rm -rf "$WORK_ROOT"` line, and bump `--time=` to the needed walltime before resubmitting.

## Release And Reporting

Once `build-core` has produced `release_files` and `site_files`:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-splits
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli audit-leakage
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli train-task-a
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli train-task-b
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli make-adjudication-set
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-site
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-report
```

Artifacts land under `artifacts/real_release/` and `artifacts/real_site/`.

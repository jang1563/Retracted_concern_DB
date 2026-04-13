# Life-Science Integrity Signals Benchmark v1

This repository contains a runnable scaffold for a research-integrity triage benchmark and a read-only evidence browser. The benchmark is designed to surface papers that may merit further scrutiny. It is explicitly not a fraud detector and it does not determine misconduct.

## What is included

- A reproducible benchmark-core build pipeline
- Rights-aware source joining and label derivation
- Horizon-based Task A targets for early-warning ranking
- Task B evidence aggregation and tiering
- Leakage auditing with explicit feature cutoff tracking for Task A
- Time splits plus grouped holdout split manifests for author clusters, venues, and publishers
- Baseline model hooks for metadata, abstract text, and metadata+text fusion
- A static evidence browser with policy, change log, and dispute workflow pages
- An internal-only curation queue artifact for non-notice external signals
- A restartable local snapshot ingest pipeline for OpenAlex bulk shards, local official-notice exports, and PubMed DOI joins
- Provider-aware notice parsing for generic rows plus Crossref/Crossmark-style nested updates
- A benchmark experiment report generator
- Sample synthetic source data so the full stack can run offline
- Vendor-archive collection and raw-snapshot staging helpers for OpenAlex, Crossref, Retraction Watch, and PubMed

## Quickstart

Create the sample release and site:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli demo
```

Or run the steps separately:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli bootstrap-sample
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-core
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-splits
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli audit-leakage
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli train-task-a
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli train-task-b
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli make-adjudication-set
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-site
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli build-report
```

Register, ingest, and materialize a local full-corpus snapshot:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli scaffold-real-ingest --raw-dir data/raw/my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli register-snapshot --snapshot-id my_snapshot --raw-root data/raw/my_snapshot --source-family openalex_notices
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector openalex_bulk
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector local_notice_export
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector pubmed_index
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli materialize-canonical --snapshot-id my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli validate-snapshot --snapshot-id my_snapshot --release-dir artifacts/real_release
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli --source-dir data/normalized/my_snapshot/canonical --release-dir artifacts/real_release --site-dir artifacts/real_site build-core
```

If you want the ingest registry and normalized shards to live outside the repo checkout, add `--root-dir`:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli --root-dir /tmp/lsib-runtime register-snapshot --snapshot-id my_snapshot --raw-root /tmp/raw_snapshot --source-family openalex_notices
```

The legacy compatibility wrapper still exists if you want flat `articles.jsonl` files:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli normalize-real-sources --raw-dir data/raw_real --normalized-dir data/sources_real
```

Build an ingest-ready raw snapshot from a vendor archive:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli validate-vendor-archive --vendor-root /data/vendor_archive --snapshot-label 2026-03-freeze
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli stage-vendor-archive --vendor-root /data/vendor_archive --raw-root /data/raw_snapshot --snapshot-label 2026-03-freeze
```

Collection wrappers for upstream sources live under `scripts/raw_snapshot/`.
If you already have a populated `vendor_archive`, you can run validation, staging, and the isolated local dry run in one command:

```bash
./scripts/raw_snapshot/run_vendor_archive_pipeline.sh /data/vendor_archive /data/raw_snapshot /tmp/lsib-vendor-dryrun 2026-03-freeze
```

If you want to run collection + staging + isolated local dry run as one monthly-freeze workflow:

```bash
cp env/raw_snapshot.env.example /tmp/raw_snapshot.env
$EDITOR /tmp/raw_snapshot.env
export RAW_SNAPSHOT_ENV_FILE="/tmp/raw_snapshot.env"

# On module-based HPC systems, aws is commonly provided as a module:
# module load awscli/2.2.14

./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_monthly_freeze.sh /data/vendor_archive /data/raw_snapshot /tmp/lsib-freeze-run 2026-03-freeze
```

If you want to stay fully free/open-data-only and skip Crossref Metadata Plus:

```bash
cp env/raw_snapshot.env.example /tmp/raw_snapshot.env
$EDITOR /tmp/raw_snapshot.env
export RAW_SNAPSHOT_ENV_FILE="/tmp/raw_snapshot.env"
export CROSSREF_SOURCE_MODE="skip"

./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_open_data_freeze.sh /data/vendor_archive /data/raw_snapshot /tmp/lsib-freeze-run 2026-03-freeze
```

`collect_open_data_freeze.sh` defaults to `symlink` staging mode to keep scratch usage low. Pass `copy` as the fifth argument if you need a physically separate copy.

To watch the HPC public-source collection job:

```bash
./scripts/cayuga/watch_public_vendor_collection_from_local.sh <user>@<hpc-login> <hpc-scratch-root>/lsib/<run-id> 60 10
```

## HPC overnight rehearsal

For the current HPC rehearsal workflow, use the helper scripts in `scripts/cayuga/` and the runbook in `docs/cayuga_overnight_run.md`.

Typical sequence:

```bash
REMOTE_HOST="<user>@<hpc-login>"
RUN_ROOT="<hpc-scratch-root>/lsib/<run-id>"

./scripts/cayuga/sync_repo_to_cayuga.sh "$REMOTE_HOST" "$RUN_ROOT"
ssh "$REMOTE_HOST"
cd "$RUN_ROOT/repo"
./scripts/cayuga/setup_run_root.sh "$RUN_ROOT"
./scripts/cayuga/submit_overnight.sh "$RUN_ROOT"
```

This submits only the `preflight` and `sample_stress` rehearsal jobs. The real-data ingest job is rendered but intentionally not submitted until raw snapshot files exist under `raw/real_snapshot/`.

If you want to do sync + remote setup + runtime check + overnight submission from the local machine in one step:

```bash
./scripts/cayuga/launch_overnight_from_local.sh "$REMOTE_HOST" "$RUN_ROOT"
```

When raw files are ready on Cayuga, check the snapshot layout and submit the real ingest job with:

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

The local dry-run helper now keeps manifests and normalized shards under `/tmp/lsib-real-dryrun/runtime_root`, so it does not write ingest state into the repo checkout.

If your source files are mixed together in a downloads folder first, you can classify and stage them with:

```bash
MIXED_SOURCE_ROOT="/path/to/downloads_or_exports"
./scripts/cayuga/classify_mixed_sources.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/review_unknown_classification.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/write_unknown_override_template.sh "$MIXED_SOURCE_ROOT/source_classification.tsv"
./scripts/cayuga/stage_mixed_sources_into_raw_snapshot.sh "$MIXED_SOURCE_ROOT" "$LOCAL_RAW_ROOT" copy "$MIXED_SOURCE_ROOT/source_classification.tsv" "$MIXED_SOURCE_ROOT/classification_overrides.tsv"
```

To watch a submitted real ingest job from the local machine:

```bash
./scripts/cayuga/watch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 20
```

Or, from the local machine:

```bash
./scripts/cayuga/launch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_RAW_ROOT"
```

For next-morning review, collect a single text summary with:

```bash
./scripts/cayuga/collect_morning_status.sh "$RUN_ROOT"
```

To cancel any recorded Cayuga jobs from the cluster side:

```bash
./scripts/cayuga/cancel_recorded_jobs.sh "$RUN_ROOT"
```

Artifacts are written to:

- `artifacts/sample_release/`
- `artifacts/site/`

## Project layout

- `src/life_science_integrity_benchmark/`: package code
- `scripts/raw_snapshot/`: vendor-archive collection, validation, and raw-snapshot staging helpers
- `docs/`: data card, rights matrix, evaluation protocol, raw-source schema, and governance policy
- `tests/`: unit tests for dataset logic, auditing, modeling, and site generation

## Important defaults

- Negative state is `none_known_at_snapshot`, never "clean"
- Public site pages never show numeric risk scores
- Official notices can auto-publish to the site
- Non-notice external signals require curator review before public display
- Extension signals are link-only unless explicit redistribution rights exist
- `year_imputed` publication dates are excluded from the primary Task A benchmark and emitted into a noisy-date analysis split

## Notes on model backends

The repository runs fully offline with built-in pure-Python baselines. The abstract encoder baseline can optionally use a locally cached Transformer model, and the metadata baseline can optionally use XGBoost if that dependency is available. Neither is required for the default demo path.

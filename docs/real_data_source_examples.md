# Real Data Source Examples

This document is a practical memo that helps you quickly decide what kinds of files to place in each `raw snapshot` directory when populating it with real data.

## Directory intent

- `openalex/`
  - OpenAlex bulk works shard
  - raw JSONL, JSONL.GZ, or OpenAlex vendor shard `.gz`
- `official_notices/`
  - Crossref / Crossmark / local notice export
  - CSV or JSONL
- `pubmed/`
  - PubMed XML baseline/update, DOI-joined CSV/JSON
  - Rows without a DOI are quarantined by the collector

## What to stage first

The smallest successful path uses the three inputs below:

1. At least one DOI-bearing work shard under `openalex/`
2. At least one DOI-based notice export under `official_notices/`
3. At least one DOI-bearing joinable file under `pubmed/`

With those three in place you can run the full `register-snapshot -> ingest-snapshot -> materialize-canonical -> validate-snapshot -> build-core` chain.

## OpenAlex examples

Recommended files:

- `works-part-000.jsonl.gz`
- `works-biology-2024.jsonl.gz`
- `000_part_000.gz`

Optional sidecars:

- `source_versions.json`
- `sha256_manifest.tsv`
- `manifest`
- `LICENSE.txt`
- `RELEASE_NOTES.txt`

Representative fields the collector actually reads:

- `doi`
- `ids.doi`
- `title` or `display_name`
- `abstract` or `abstract_inverted_index`
- `publication_date` or `publication_year`
- `type` or `type_crossref`
- `authorships`
- `concepts`
- `primary_location`
- `locations`
- `publisher`

Avoid:

- Separate metadata dumps without DOIs
- Exports that are overwhelmingly non-`article` / non-`review` work types

## Official notice examples

Recommended files:

- `crossref_retractions_2026-04-10.csv`
- `crossmark_updates.jsonl`
- `local_notice_export.jsonl.gz`

Key field aliases for flat rows:

- `doi`
- `notice_type` / `tag` / `notice_label`
- `notice_date` / `date`
- `source_name` / `source`
- `source_url` / `url`

Supported canonical notice types:

- `retraction`
- `expression_of_concern`
- `major_correction`

Provider-like nested rows are also supported:

- `relation.is-retraction-of`
- `update-to` / `update_to`
- Crossref `date-parts`

Avoid:

- Free-text allegation sheets with no DOIs
- Files whose notice labels cannot be mapped to a canonical type

## PubMed examples

Recommended files:

- `pubmed_baseline_2026.xml.gz`
- `pubmed_updatefiles_2026-04.xml.gz`
- `pubmed_doi_join.csv.gz`

Normalized fields the collector can emit:

- DOI
- PMID
- MeSH terms
- keywords
- publication types
- journal title

Best inputs:

- PubMed CSV/JSON that already carries DOIs
- Standard PubMed XML

Avoid:

- PMID-only tables without DOIs
- Summary exports missing article-level identifiers

## Minimal sanity checklist

Recommended local sequence before uploading:

```bash
LOCAL_RAW_ROOT="/path/to/local/raw_snapshot"

./scripts/cayuga/scaffold_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/check_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/inventory_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
```

If files are mixed across folders, classify and stage them first:

```bash
MIXED_SOURCE_ROOT="/path/to/downloads_or_exports"
LOCAL_RAW_ROOT="/path/to/local/raw_snapshot"

./scripts/cayuga/classify_mixed_sources.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/review_unknown_classification.sh "$MIXED_SOURCE_ROOT"
./scripts/cayuga/write_unknown_override_template.sh "$MIXED_SOURCE_ROOT/source_classification.tsv"
# edit the override TSV if needed
./scripts/cayuga/stage_mixed_sources_into_raw_snapshot.sh "$MIXED_SOURCE_ROOT" "$LOCAL_RAW_ROOT" copy "$MIXED_SOURCE_ROOT/source_classification.tsv" "$MIXED_SOURCE_ROOT/classification_overrides.tsv"
./scripts/cayuga/check_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
./scripts/cayuga/inventory_local_raw_snapshot.sh "$LOCAL_RAW_ROOT"
```

Default staging mode is `copy`. Pass `symlink` as the third argument to link instead of copy.

For ambiguous files, inspect samples with `review_unknown_classification.sh` first. To assign a manual bucket, generate a template with `write_unknown_override_template.sh`, then apply it via `apply_classification_overrides.sh` or `stage_mixed_sources_into_raw_snapshot.sh ... <override_tsv>`.

Then upload to the HPC cluster:

```bash
REMOTE_HOST="<user>@<hpc-login>"
RUN_ROOT="<hpc-scratch-root>/lsib/<run-id>"

./scripts/cayuga/sync_real_snapshot_to_cayuga.sh "$REMOTE_HOST" "$RUN_ROOT" "$LOCAL_RAW_ROOT"
./scripts/cayuga/check_real_snapshot_ready.sh "$RUN_ROOT"
./scripts/cayuga/submit_real_ingest.sh "$RUN_ROOT"
./scripts/cayuga/watch_real_ingest_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 20
```

To verify the collector / validation / build locally before uploading:

```bash
LOCAL_RAW_ROOT="/path/to/local/raw_snapshot"
LOCAL_WORK_ROOT="/tmp/lsib-real-dryrun"

./scripts/cayuga/run_local_real_snapshot_pipeline.sh "$LOCAL_RAW_ROOT" "$LOCAL_WORK_ROOT"
```

This script runs `register-snapshot -> ingest-snapshot -> materialize-canonical -> validate-snapshot -> build-core -> build-splits -> audit-leakage` locally. Ingest manifests and normalized shards are written under `$LOCAL_WORK_ROOT/runtime_root`, so the repo checkout's `data/` is left untouched.

## Vendor archive -> raw snapshot

For the maximum-coverage path, preserve upstream raw artifacts under `vendor_archive/` first, then stage an ingest-ready `raw_snapshot/` from it.

Example:

```bash
VENDOR_ROOT="/data/vendor_archive"
RAW_ROOT="/data/raw_snapshot"
SNAPSHOT_LABEL="2026-03-freeze"

./scripts/raw_snapshot/check_vendor_archive_snapshot.sh "$VENDOR_ROOT" "$SNAPSHOT_LABEL"
./scripts/raw_snapshot/stage_vendor_archive_to_raw_snapshot.sh "$VENDOR_ROOT" "$RAW_ROOT" "$SNAPSHOT_LABEL"
./scripts/cayuga/check_local_raw_snapshot.sh "$RAW_ROOT"
```

Upstream collection wrappers live at:

- `scripts/raw_snapshot/collect_openalex_snapshot.sh`
- `scripts/raw_snapshot/collect_crossref_monthly_snapshot.sh`
- `scripts/raw_snapshot/collect_retraction_watch_csv.sh`
- `scripts/raw_snapshot/collect_pubmed_baseline_updatefiles.sh`

If `vendor_archive` is already populated, run validation, staging, and the local dry run in one command:

```bash
./scripts/raw_snapshot/run_vendor_archive_pipeline.sh "$VENDOR_ROOT" "$RAW_ROOT" /tmp/lsib-vendor-dryrun "$SNAPSHOT_LABEL"
```

To drive collection end-to-end:

```bash
cp env/raw_snapshot.env.example /tmp/raw_snapshot.env
$EDITOR /tmp/raw_snapshot.env
export RAW_SNAPSHOT_ENV_FILE="/tmp/raw_snapshot.env"

# HPC example:
# module load awscli/2.2.14

./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_monthly_freeze.sh "$VENDOR_ROOT" "$RAW_ROOT" /tmp/lsib-freeze-run "$SNAPSHOT_LABEL"
```

To stay on free / open-data sources only:

```bash
cp env/raw_snapshot.env.example /tmp/raw_snapshot.env
$EDITOR /tmp/raw_snapshot.env
export RAW_SNAPSHOT_ENV_FILE="/tmp/raw_snapshot.env"
export CROSSREF_SOURCE_MODE="skip"

./scripts/raw_snapshot/check_collection_runtime.sh
./scripts/raw_snapshot/collect_open_data_freeze.sh "$VENDOR_ROOT" "$RAW_ROOT" /tmp/lsib-freeze-run "$SNAPSHOT_LABEL"
```

`collect_open_data_freeze.sh` defaults to `symlink` mode. Pass `copy` as the fifth argument only when you need a physically separate copy from the vendor archive.

To monitor the free / open-data-only collection job on the HPC cluster:

```bash
./scripts/cayuga/watch_public_vendor_collection_from_local.sh "$REMOTE_HOST" "$RUN_ROOT" 60 10
```

## Recommended first real-data run

It's safer to start smaller than a full dump for the first real ingest:

- A handful of shards under `openalex/`
- A small CSV/JSONL under `official_notices/`
- A small DOI-bearing XML or CSV under `pubmed/`

Once end-to-end passes on this small snapshot, scale up to the full snapshot.

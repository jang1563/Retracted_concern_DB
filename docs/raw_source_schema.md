# Raw Source Schema

## Goal

This document defines the expected local snapshot layout for the full-corpus local ingest system.

For a more operational “what files should I actually put here?” guide, see `docs/real_data_source_examples.md`.

## Required Directory Layout

- `openalex/`
- `official_notices/`
- `pubmed/`

The current collectors scan recursively within those directories.

Allowed sidecar files that are ignored by ingest registration and readiness checks:

- `source_versions.json`
- `sha256_manifest.tsv`
- `fetch.log`
- `openalex/manifest`
- `openalex/LICENSE.txt`
- `openalex/RELEASE_NOTES.txt`

Collection wrappers may also load environment variables from `RAW_SNAPSHOT_ENV_FILE`.
An example file lives at [env/raw_snapshot.env.example](../env/raw_snapshot.env.example).
Set `CROSSREF_SOURCE_MODE=skip` to run the free/open-data-only path without Metadata Plus.

## Supported File Types

- `openalex_bulk`
  - `.jsonl`
  - `.jsonl.gz`
  - `.gz`
- `local_notice_export`
  - `.jsonl`
  - `.jsonl.gz`
  - `.csv`
  - `.csv.gz`
- `pubmed_index`
  - `.jsonl`
  - `.jsonl.gz`
  - `.csv`
  - `.csv.gz`
  - `.xml`
  - `.xml.gz`

## OpenAlex Row Requirements

Each OpenAlex-style work row should provide enough information to reconstruct:

- DOI
- title
- abstract or abstract inverted index
- publication date or year
- work type
- concepts
- authorships
- venue and publisher
- reference count
- open-access status

Rows without a DOI, rows with unsupported work types, or rows with invalid dates are quarantined rather than promoted to canonical benchmark input. Life-science eligibility is finalized after optional PubMed DOI joins.

## Official Notice Row Requirements

The local notice export collector supports these field aliases:

- DOI: `doi`
- notice type: `notice_type | tag | notice_label`
- notice date: `notice_date | date`
- source name: `source_name | source`
- source URL: `source_url | url`
- rights: `rights_status`

Canonical notice types are:

- `retraction`
- `expression_of_concern`
- `major_correction`

Unknown notice types are quarantined and preserved in audit statistics.

The collector also recognizes provider-style nested exports, including:

- Crossref relation rows such as `relation.is-retraction-of`
- Crossmark article rows with `update-to` / `update_to`
- Crossref-style date objects with `date-parts`

## PubMed Join Row Requirements

The `pubmed_index` collector accepts either flat JSON/CSV rows or PubMed XML.

Supported normalized join fields include:

- DOI
- PMID
- MeSH terms
- keywords
- publication types
- journal title

Rows without a DOI are quarantined because the current join key is DOI.

## Snapshot Commands

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli scaffold-real-ingest --raw-dir data/raw/my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli register-snapshot --snapshot-id my_snapshot --raw-root data/raw/my_snapshot --source-family openalex_notices
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector openalex_bulk
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector local_notice_export
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector pubmed_index
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli materialize-canonical --snapshot-id my_snapshot
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli validate-snapshot --snapshot-id my_snapshot
```

If you want an isolated runtime root, add `--root-dir /path/to/runtime_root` to any ingest-related command.

To produce an ingest-ready raw snapshot from a vendor archive:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli validate-vendor-archive --vendor-root /data/vendor_archive --snapshot-label 2026-03-freeze
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli stage-vendor-archive --vendor-root /data/vendor_archive --raw-root /data/raw_snapshot --snapshot-label 2026-03-freeze
```

## Design Notes

- Raw files stay local and are not redistributed by the benchmark
- Registration freezes the snapshot with byte-level file hashes in `data/manifests/ingest.sqlite3`
- Normalized collector outputs are written as gzip-compressed JSONL shards under `data/normalized/<snapshot_id>/<collector_name>/`
- Canonical outputs are written under `data/normalized/<snapshot_id>/canonical/`
- Orphan notices are preserved in `canonical/orphan_notices/`
- PubMed DOI joins are applied during canonical materialization
- History-count features are re-derived from prior observed events during canonical materialization
- `year_imputed` publication dates are routed to a separate noisy-date analysis split for Task A

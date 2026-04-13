"""Snapshot registration, collector ingest, and legacy compatibility wrappers."""

import hashlib
from pathlib import Path
from typing import Dict, List, Optional

from .collectors import get_collector
from .constants import (
    MANIFEST_DB_PATH,
    NOTICE_COLLECTOR,
    OPENALEX_COLLECTOR,
    PARSER_BUNDLE_VERSION,
    PUBMED_COLLECTOR,
    SNAPSHOT_DATE,
    SOURCE_FAMILY_OPENALEX_NOTICES,
)
from .manifest import ManifestStore, SnapshotModifiedError
from .materialize import materialize_canonical_snapshot
from .utils import normalize_doi, slugify, write_jsonl


RAW_TEMPLATE_LAYOUT = {
    "openalex/openalex_works.example.jsonl": {
        "doi": "https://doi.org/10.1000/example.1",
        "display_name": "Example life-science article",
        "abstract_inverted_index": {"Example": [0], "life-science": [1], "article": [2]},
        "publication_year": 2023,
        "type": "article",
        "concepts": [{"display_name": "Biology", "score": 0.92}],
        "authorships": [
            {
                "author": {"display_name": "Alex Example"},
                "institutions": [{"display_name": "Example Institute"}],
            }
        ],
        "host_venue": {"display_name": "Example Journal"},
        "publisher": "Example Publisher",
        "referenced_works_count": 24,
        "is_oa": True,
    },
    "official_notices/official_notices.example.jsonl": {
        "doi": "10.1000/example.1",
        "notice_type": "expression_of_concern",
        "notice_date": "2024-02-10",
        "source_name": "Crossmark",
        "source_url": "https://example.org/notice",
        "rights_status": "metadata_only",
    },
    "pubmed/pubmed_index.example.jsonl": {
        "doi": "10.1000/example.1",
        "pmid": "12345678",
        "mesh_terms": ["Biology", "Genomics"],
        "keywords": ["benchmark", "transcriptomics"],
        "publication_types": ["Journal Article"],
        "journal_title": "Example Journal",
        "is_pubmed_indexed": True,
    },
}


def scaffold_real_source_layout(raw_dir: Path) -> Dict[str, Path]:
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    readme_path = raw_dir / "README.md"
    readme_path.write_text(_raw_layout_readme(), encoding="utf-8")
    paths["README"] = readme_path
    for relative_path, row in RAW_TEMPLATE_LAYOUT.items():
        path = raw_dir / relative_path
        write_jsonl(path, [row])
        paths[relative_path] = path
    return paths


def register_snapshot(
    snapshot_id: str,
    raw_root: Path,
    root_dir: Optional[Path] = None,
    source_family: str = SOURCE_FAMILY_OPENALEX_NOTICES,
    snapshot_date: str = SNAPSHOT_DATE,
    snapshot_label: Optional[str] = None,
) -> Dict[str, object]:
    store = _manifest_store(root_dir)
    return store.register_snapshot(
        snapshot_id=snapshot_id,
        raw_root=raw_root,
        source_family=source_family,
        snapshot_date=snapshot_date,
        snapshot_label=snapshot_label,
        parser_bundle_version=PARSER_BUNDLE_VERSION,
    )


def ingest_snapshot(
    snapshot_id: str,
    collector_name: str,
    root_dir: Optional[Path] = None,
) -> Dict[str, object]:
    root_dir = Path(root_dir or Path.cwd())
    store = _manifest_store(root_dir)
    store.assert_snapshot_frozen(snapshot_id)
    snapshot = store.get_snapshot(snapshot_id)
    collector = get_collector(collector_name)
    file_rows = {row["file_id"]: row for row in store.list_files(snapshot_id, collector_name)}
    files = collector.discover_files(Path(snapshot["raw_root"]), store, snapshot_id)
    normalized_dir = root_dir / "data" / "normalized" / snapshot_id / collector_name
    quarantine_dir = root_dir / "data" / "quarantine" / snapshot_id / collector_name
    normalized_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    run_id = store.start_run(snapshot_id, "ingest_snapshot", collector_name)
    processed_files = 0
    try:
        for file_meta in files:
            file_row = file_rows[file_meta.file_id]
            normalized_path = normalized_dir / ("%s.jsonl.gz" % file_meta.content_sha256)
            quarantine_path = quarantine_dir / ("%s.jsonl.gz" % file_meta.content_sha256)
            if (
                file_row["parse_status"] == "success"
                and normalized_path.exists()
                and (quarantine_path.exists() or int(file_row["quarantined_rows"]) == 0)
            ):
                continue

            normalized_rows: List[dict] = []
            quarantined_rows: List[dict] = []
            row_errors: List[dict] = []
            for line_number, raw_record in collector.iter_raw_records(file_meta):
                result = collector.normalize_record(
                    raw_record,
                    context={
                        "snapshot_id": snapshot_id,
                        "file_id": file_meta.file_id,
                        "line_number": line_number,
                    },
                )
                if result["kind"] == "normalized":
                    normalized_rows.append(result["row"])
                elif result["kind"] == "normalized_many":
                    normalized_rows.extend(result["rows"])
                else:
                    quarantine_items = result.get("rows", [result["row"]])
                    for item in quarantine_items:
                        quarantine_row = dict(item)
                        quarantined_rows.append(quarantine_row)
                        row_errors.append(
                            {
                                "line_number": quarantine_row["line_number"],
                                "error_code": quarantine_row["error_code"],
                                "error_message": quarantine_row["error_message"],
                                "raw_excerpt": quarantine_row["raw_excerpt"],
                            }
                        )

            write_jsonl(normalized_path, normalized_rows)
            write_jsonl(quarantine_path, quarantined_rows)
            store.update_file_parse_result(
                file_id=file_meta.file_id,
                parse_status="success",
                parsed_rows=len(normalized_rows),
                quarantined_rows=len(quarantined_rows),
                error_count=len(row_errors),
            )
            store.replace_row_errors(file_meta.file_id, row_errors)
            store.upsert_artifact(
                snapshot_id,
                "normalized_%s" % collector_name,
                str(normalized_path.relative_to(root_dir)),
                len(normalized_rows),
            )
            store.upsert_artifact(
                snapshot_id,
                "quarantine_%s" % collector_name,
                str(quarantine_path.relative_to(root_dir)),
                len(quarantined_rows),
            )
            processed_files += 1
        store.finish_run(run_id, "success")
    except Exception:
        store.finish_run(run_id, "failed")
        raise
    return {
        "snapshot_id": snapshot_id,
        "collector": collector_name,
        "processed_files": processed_files,
        "normalized_dir": normalized_dir,
        "quarantine_dir": quarantine_dir,
    }


def normalize_real_source_exports(raw_dir: Path, normalized_dir: Path) -> Dict[str, Path]:
    """Compatibility wrapper over the new snapshot ingest flow.

    This keeps the old helper alive for tests and quick local demos while ensuring
    the actual implementation runs through snapshot registration, collector ingest,
    and canonical materialization.
    """

    raw_dir = Path(raw_dir)
    normalized_dir = Path(normalized_dir)
    root_dir = normalized_dir.parent
    snapshot_id = _legacy_snapshot_id(raw_dir, normalized_dir)
    register_snapshot(
        snapshot_id=snapshot_id,
        raw_root=raw_dir,
        root_dir=root_dir,
        source_family=SOURCE_FAMILY_OPENALEX_NOTICES,
        snapshot_date=SNAPSHOT_DATE,
        snapshot_label="legacy-normalize",
    )
    for collector_name in (OPENALEX_COLLECTOR, NOTICE_COLLECTOR, PUBMED_COLLECTOR):
        try:
            ingest_snapshot(snapshot_id=snapshot_id, collector_name=collector_name, root_dir=root_dir)
        except KeyError:
            continue
    materialized = materialize_canonical_snapshot(
        snapshot_id=snapshot_id,
        root_dir=root_dir,
        manifest=_manifest_store(root_dir),
    )

    articles = _read_all_rows(materialized["articles_dir"])
    notices = _read_all_rows(materialized["official_notices_dir"])
    write_jsonl(normalized_dir / "articles.jsonl", articles)
    write_jsonl(normalized_dir / "official_notices.jsonl", notices)
    write_jsonl(normalized_dir / "external_signals.jsonl", [])
    return {
        "articles": normalized_dir / "articles.jsonl",
        "official_notices": normalized_dir / "official_notices.jsonl",
        "external_signals": normalized_dir / "external_signals.jsonl",
        "canonical_root": materialized["canonical_root"],
    }


def _manifest_store(root_dir: Optional[Path]) -> ManifestStore:
    root_dir = Path(root_dir or Path.cwd())
    return ManifestStore(root_dir / MANIFEST_DB_PATH)


def _legacy_snapshot_id(raw_dir: Path, normalized_dir: Path) -> str:
    digest = hashlib.sha1(
        ("%s|%s" % (raw_dir.resolve(), normalized_dir.resolve())).encode("utf-8")
    ).hexdigest()[:12]
    return "legacy-%s" % digest


def _read_all_rows(directory: Path) -> List[dict]:
    rows = []
    for path in sorted(directory.glob("*.jsonl.gz")):
        from .utils import read_jsonl

        rows.extend(read_jsonl(path))
    return rows


def _raw_layout_readme() -> str:
    return """# Raw Source Snapshot Layout

Place local snapshot files under:

- `openalex/` for OpenAlex bulk-style `.jsonl`, `.jsonl.gz`, or vendor `.gz` shards
- `official_notices/` for notice exports in `.jsonl`, `.jsonl.gz`, `.csv`, or `.csv.gz`
- `pubmed/` for PubMed DOI-index exports in `.jsonl`, `.jsonl.gz`, `.csv`, `.csv.gz`, `.xml`, or `.xml.gz`

Allowed sidecar files:

- `source_versions.json`
- `sha256_manifest.tsv`
- `fetch.log`
- `openalex/manifest`
- `openalex/LICENSE.txt`
- `openalex/RELEASE_NOTES.txt`

Then run:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli register-snapshot --snapshot-id my_snapshot --raw-root data/raw/my_snapshot --source-family openalex_notices
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector openalex_bulk
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector local_notice_export
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli ingest-snapshot --snapshot-id my_snapshot --collector pubmed_index
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli materialize-canonical --snapshot-id my_snapshot
```

The compatibility command below still works and writes legacy flat files:

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli normalize-real-sources --raw-dir data/raw_real --normalized-dir data/sources_real
```
"""

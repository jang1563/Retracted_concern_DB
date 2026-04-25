"""Validation checks for frozen canonical snapshots."""

from pathlib import Path
from typing import Dict, List

from .constants import ALLOWED_NOTICE_TYPES
from .manifest import ManifestStore
from .utils import count_jsonl_rows, parse_date, read_json, read_jsonl


def validate_snapshot(snapshot_id: str, root_dir: Path, manifest: ManifestStore) -> Dict[str, object]:
    manifest.assert_snapshot_frozen(snapshot_id)
    normalized_root = Path(root_dir) / "data" / "normalized" / snapshot_id
    canonical_root = normalized_root / "canonical"
    article_rows = _read_shard_rows(canonical_root / "articles")
    notice_rows = _read_shard_rows(canonical_root / "official_notices")
    orphan_notice_rows = _read_shard_rows(canonical_root / "orphan_notices")
    summary = read_json(canonical_root / "collection_summary.json")
    snapshot = manifest.get_snapshot(snapshot_id)
    snapshot_date = parse_date(snapshot["snapshot_date"])

    violations: List[str] = []
    dois = [row["doi"] for row in article_rows]
    if len(dois) != len(set(dois)):
        violations.append("duplicate DOI remains in canonical articles")

    publication_by_doi = {row["doi"]: row["publication_date"] for row in article_rows}
    for notice in notice_rows:
        if notice["notice_type"] not in ALLOWED_NOTICE_TYPES:
            violations.append("invalid canonical notice_type: %s" % notice["notice_type"])
        publication = publication_by_doi.get(notice["doi"])
        if publication and parse_date(notice["notice_date"]) < parse_date(publication):
            violations.append("event date earlier than publication: %s" % notice["doi"])
        if parse_date(notice["notice_date"]) > snapshot_date:
            violations.append("event date later than snapshot: %s" % notice["doi"])

    for row in article_rows:
        if parse_date(row["publication_date"]) > snapshot_date:
            violations.append("article publication date later than snapshot: %s" % row["doi"])
        if (
            row.get("task_a_date_bucket") == "primary"
            and row.get("publication_date_precision") == "year_imputed"
        ):
            violations.append("primary Task A row has year-imputed date: %s" % row["doi"])

    _validate_summary_count(
        summary,
        "canonical_article_count",
        len(article_rows),
        violations,
    )
    _validate_summary_count(
        summary,
        "canonical_notice_count",
        len(notice_rows),
        violations,
    )
    _validate_summary_count(
        summary,
        "orphan_notice_count",
        len(orphan_notice_rows),
        violations,
    )

    file_rows = manifest.list_files(snapshot_id)
    for file_row in file_rows:
        normalized_path = (
            Path(root_dir)
            / "data"
            / "normalized"
            / snapshot_id
            / file_row["collector_name"]
            / ("%s.jsonl.gz" % file_row["content_sha256"])
        )
        quarantine_path = (
            Path(root_dir)
            / "data"
            / "quarantine"
            / snapshot_id
            / file_row["collector_name"]
            / ("%s.jsonl.gz" % file_row["content_sha256"])
        )
        if normalized_path.exists():
            if count_jsonl_rows(normalized_path) != int(file_row["parsed_rows"]):
                violations.append("normalized shard count mismatch: %s" % file_row["relative_path"])
        elif int(file_row["parsed_rows"]) != 0:
            violations.append("missing normalized shard: %s" % file_row["relative_path"])
        if quarantine_path.exists():
            if count_jsonl_rows(quarantine_path) != int(file_row["quarantined_rows"]):
                violations.append("quarantine shard count mismatch: %s" % file_row["relative_path"])
        elif int(file_row["quarantined_rows"]) != 0:
            violations.append("missing quarantine shard: %s" % file_row["relative_path"])

    return {
        "snapshot_id": snapshot_id,
        "snapshot_date": snapshot["snapshot_date"],
        "article_count": len(article_rows),
        "notice_count": len(notice_rows),
        "orphan_notice_count": len(orphan_notice_rows),
        "passed": not violations,
        "violations": violations,
    }


def _read_shard_rows(directory: Path) -> List[dict]:
    rows = []
    if not directory.exists():
        return rows
    for path in sorted(directory.glob("*.jsonl.gz")):
        rows.extend(read_jsonl(path))
    return rows


def _validate_summary_count(
    summary: dict, field_name: str, actual_count: int, violations: List[str]
) -> None:
    if field_name not in summary:
        violations.append("collection summary missing %s" % field_name)
        return
    try:
        expected_count = int(summary[field_name])
    except (TypeError, ValueError):
        violations.append("collection summary invalid %s: %r" % (field_name, summary[field_name]))
        return
    if expected_count != actual_count:
        violations.append(
            "collection summary %s mismatch: summary=%d actual=%d"
            % (field_name, expected_count, actual_count)
        )

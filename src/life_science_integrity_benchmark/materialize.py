"""Canonical snapshot materialization from normalized collector shards."""

import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .constants import (
    CANONICAL_SHARD_SIZE,
    DATE_PRECISION_ORDER,
    NOTICE_COLLECTOR,
    OPENALEX_COLLECTOR,
    PUBMED_COLLECTOR,
)
from .manifest import ManifestStore
from .utils import coerce_bool, parse_date, read_jsonl, write_json, write_jsonl


def materialize_canonical_snapshot(
    snapshot_id: str, root_dir: Path, manifest: ManifestStore
) -> Dict[str, Path]:
    _assert_snapshot_frozen_unless_trusted(manifest, snapshot_id)
    normalized_root = Path(root_dir) / "data" / "normalized" / snapshot_id
    canonical_root = normalized_root / "canonical"
    canonical_root.mkdir(parents=True, exist_ok=True)

    article_rows = _read_collector_rows(normalized_root / OPENALEX_COLLECTOR)
    notice_rows = _read_collector_rows(normalized_root / NOTICE_COLLECTOR)
    pubmed_rows = _read_collector_rows(normalized_root / PUBMED_COLLECTOR)

    articles, duplicate_doi_count = _merge_articles(snapshot_id, article_rows)
    pubmed_join_count = _join_pubmed_metadata(articles, pubmed_rows)
    notices, orphan_notices = _match_notices(articles, notice_rows)
    _recompute_history_counts(articles, notices)

    article_paths = _write_shards(canonical_root / "articles", articles, "part")
    notice_paths = _write_shards(
        canonical_root / "official_notices",
        notices,
        "part",
    )
    orphan_paths = _write_shards(
        canonical_root / "orphan_notices",
        orphan_notices,
        "part",
    )

    manifest.replace_artifacts(
        snapshot_id,
        "canonical_articles",
        [(str(path.relative_to(root_dir)), len(rows)) for path, rows in article_paths],
    )
    manifest.replace_artifacts(
        snapshot_id,
        "canonical_official_notices",
        [(str(path.relative_to(root_dir)), len(rows)) for path, rows in notice_paths],
    )
    manifest.replace_artifacts(
        snapshot_id,
        "canonical_orphan_notices",
        [(str(path.relative_to(root_dir)), len(rows)) for path, rows in orphan_paths],
    )

    summary = _build_collection_summary(
        snapshot_id=snapshot_id,
        manifest=manifest,
        articles=articles,
        notices=notices,
        orphan_notices=orphan_notices,
        duplicate_doi_count=duplicate_doi_count,
        pubmed_join_count=pubmed_join_count,
    )
    summary_path = canonical_root / "collection_summary.json"
    write_json(summary_path, summary)
    manifest.replace_artifacts(
        snapshot_id,
        "collection_summary",
        [(str(summary_path.relative_to(root_dir)), 1)],
    )
    return {
        "canonical_root": canonical_root,
        "articles_dir": canonical_root / "articles",
        "official_notices_dir": canonical_root / "official_notices",
        "orphan_notices_dir": canonical_root / "orphan_notices",
        "collection_summary": summary_path,
    }


def _assert_snapshot_frozen_unless_trusted(manifest: ManifestStore, snapshot_id: str) -> None:
    if coerce_bool(os.environ.get("LSIB_TRUST_REGISTERED_SNAPSHOT"), default=False):
        return
    manifest.assert_snapshot_frozen(snapshot_id)


def _read_collector_rows(collector_dir: Path) -> List[dict]:
    if not collector_dir.exists():
        return []
    rows = []
    for path in sorted(collector_dir.glob("*.jsonl.gz")):
        rows.extend(read_jsonl(path))
    return rows


def _merge_articles(snapshot_id: str, rows: List[dict]) -> Tuple[List[dict], int]:
    winners: Dict[str, dict] = {}
    duplicate_doi_count = 0
    for row in rows:
        doi = row["doi"]
        if doi in winners:
            duplicate_doi_count += 1
            winners[doi] = _choose_better_article(winners[doi], row)
        else:
            winners[doi] = dict(row)
        winners[doi]["ingest_snapshot_id"] = snapshot_id
    articles = sorted(winners.values(), key=lambda item: item["doi"])
    return articles, duplicate_doi_count


def _choose_better_article(left: dict, right: dict) -> dict:
    left_rank = _article_rank(left)
    right_rank = _article_rank(right)
    if right_rank > left_rank:
        winner, loser = dict(right), left
    elif left_rank > right_rank:
        winner, loser = dict(left), right
    else:
        left_key = _lineage_key(left)
        right_key = _lineage_key(right)
        if right_key < left_key:
            winner, loser = dict(right), left
        else:
            winner, loser = dict(left), right
    lineage = list(winner.get("source_lineage", []))
    lineage.extend(loser.get("source_lineage", []))
    winner["source_lineage"] = sorted(
        _dedupe_lineage(lineage),
        key=lambda item: (
            item.get("collector_name", ""),
            item.get("source_file_id", ""),
            item.get("source_line_number", 0),
        ),
    )
    return winner


def _article_rank(row: dict) -> Tuple[int, int, int, int, int, int, float]:
    return (
        1 if row.get("abstract", "").strip() else 0,
        DATE_PRECISION_ORDER.get(row.get("publication_date_precision", "day"), -1),
        len(row.get("authors", [])),
        len(row.get("institutions", [])),
        1 if row.get("venue") and row.get("venue") != "Unknown Venue" else 0,
        1 if row.get("publisher") and row.get("publisher") != "Unknown Publisher" else 0,
        float(row.get("openalex_life_science_score", 0.0)),
    )


def _lineage_key(row: dict) -> str:
    return "%s:%08d" % (
        row.get("source_file_id", ""),
        int(row.get("source_line_number", 0)),
    )


def _dedupe_lineage(entries: Iterable[dict]) -> List[dict]:
    seen = set()
    deduped = []
    for entry in entries:
        key = (
            entry.get("collector_name"),
            entry.get("source_file_id"),
            entry.get("source_line_number"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _match_notices(articles: List[dict], notice_rows: List[dict]) -> Tuple[List[dict], List[dict]]:
    article_dois = {row["doi"] for row in articles}
    deduped = {}
    for row in notice_rows:
        key = (
            row["doi"],
            row["notice_type"],
            row["notice_date"],
            row.get("source_name", ""),
            row.get("source_url", ""),
        )
        deduped.setdefault(key, dict(row))
    notices = []
    orphan_notices = []
    for row in sorted(deduped.values(), key=lambda item: (item["doi"], item["notice_date"], item["notice_type"])):
        if row["doi"] in article_dois:
            notices.append(row)
        else:
            orphan_notices.append(row)
    return notices, orphan_notices


def _join_pubmed_metadata(articles: List[dict], pubmed_rows: List[dict]) -> int:
    if not pubmed_rows:
        return 0
    best_by_doi = {}
    for row in pubmed_rows:
        doi = row["doi"]
        current = best_by_doi.get(doi)
        if current is None or _pubmed_rank(row) > _pubmed_rank(current):
            best_by_doi[doi] = dict(row)
    joined = 0
    for article in articles:
        pubmed = best_by_doi.get(article["doi"])
        if pubmed is None:
            continue
        joined += 1
        article["is_pubmed_indexed"] = coerce_bool(
            pubmed.get("is_pubmed_indexed"), default=True
        )
        article["pmid"] = str(pubmed.get("pmid", "") or "")
        article["mesh_terms"] = list(pubmed.get("mesh_terms", []))
        article["keywords"] = list(pubmed.get("keywords", []))
        article["pubmed_publication_types"] = list(pubmed.get("pubmed_publication_types", []))
        article["pubmed_journal_title"] = str(pubmed.get("pubmed_journal_title", "") or "")
        article["source_lineage"] = sorted(
            _dedupe_lineage(
                list(article.get("source_lineage", []))
                + [
                    {
                        "collector_name": PUBMED_COLLECTOR,
                        "source_file_id": pubmed.get("source_file_id", ""),
                        "source_line_number": pubmed.get("source_line_number", 0),
                    }
                ]
            ),
            key=lambda item: (
                item.get("collector_name", ""),
                item.get("source_file_id", ""),
                item.get("source_line_number", 0),
            ),
        )
        if _should_override_subfield(article.get("subfield", ""), pubmed.get("subfield_hint", "")):
            article["subfield"] = pubmed["subfield_hint"]
    return joined


def _pubmed_rank(row: dict) -> Tuple[int, int, int, str]:
    return (
        len(row.get("mesh_terms", [])),
        len(row.get("keywords", [])),
        len(row.get("pubmed_publication_types", [])),
        str(row.get("pmid", "")),
    )


def _should_override_subfield(current_subfield: str, hint_subfield: str) -> bool:
    if not hint_subfield:
        return False
    if not current_subfield:
        return True
    if current_subfield == "biology" and hint_subfield in {"biomedicine", "bioinformatics"}:
        return True
    return False


def _recompute_history_counts(articles: List[dict], notices: List[dict]) -> None:
    earliest_event = {}
    for notice in notices:
        earliest_event[notice["doi"]] = min(
            earliest_event.get(notice["doi"], notice["notice_date"]),
            notice["notice_date"],
        )

    articles_by_doi = {row["doi"]: row for row in articles}
    event_candidates = []
    for doi, event_date in earliest_event.items():
        article = articles_by_doi.get(doi)
        if article is None:
            continue
        publication = parse_date(article["publication_date"])
        event = parse_date(event_date)
        event_candidates.append((max(publication, event), doi, article))
    event_candidates.sort(key=lambda item: (item[0], item[1]))

    active_author_dois = defaultdict(set)
    active_venue_dois = defaultdict(set)
    active_publisher_dois = defaultdict(set)

    def activate(article: dict) -> None:
        doi = article["doi"]
        for author in article.get("authors", []):
            active_author_dois[author].add(doi)
        active_venue_dois[article.get("venue")].add(doi)
        active_publisher_dois[article.get("publisher")].add(doi)

    ordered = sorted(articles, key=lambda row: (row["publication_date"], row["doi"]))
    event_index = 0
    for row in ordered:
        publication = parse_date(row["publication_date"])
        while (
            event_index < len(event_candidates)
            and event_candidates[event_index][0] < publication
        ):
            activate(event_candidates[event_index][2])
            event_index += 1

        prior_author_dois = set()
        for author in row.get("authors", []):
            prior_author_dois.update(active_author_dois.get(author, ()))

        prior_journal_dois = set(active_venue_dois.get(row.get("venue"), ()))
        prior_journal_dois.update(active_publisher_dois.get(row.get("publisher"), ()))
        row["author_history_signal_count"] = len(prior_author_dois)
        row["journal_history_signal_count"] = len(prior_journal_dois)
        row["author_history_cutoff_date"] = row["publication_date"]
        row["journal_history_cutoff_date"] = row["publication_date"]
        row["task_a_date_bucket"] = (
            "noisy_date"
            if row.get("publication_date_precision") == "year_imputed"
            else "primary"
        )


def _write_shards(directory: Path, rows: List[dict], stem: str) -> List[Tuple[Path, List[dict]]]:
    directory.mkdir(parents=True, exist_ok=True)
    for stale_path in directory.glob("%s-*.jsonl.gz" % stem):
        stale_path.unlink()
    outputs = []
    if not rows:
        path = directory / ("%s-00000.jsonl.gz" % stem)
        write_jsonl(path, [])
        outputs.append((path, []))
        return outputs
    sorted_rows = sorted(rows, key=lambda item: (item.get("doi", ""), item.get("notice_date", "")))
    for shard_index in range(0, len(sorted_rows), CANONICAL_SHARD_SIZE):
        shard_rows = sorted_rows[shard_index : shard_index + CANONICAL_SHARD_SIZE]
        path = directory / ("%s-%05d.jsonl.gz" % (stem, shard_index // CANONICAL_SHARD_SIZE))
        write_jsonl(path, shard_rows)
        outputs.append((path, shard_rows))
    return outputs


def _build_collection_summary(
    snapshot_id: str,
    manifest: ManifestStore,
    articles: List[dict],
    notices: List[dict],
    orphan_notices: List[dict],
    duplicate_doi_count: int,
    pubmed_join_count: int,
) -> dict:
    file_rows = manifest.list_files(snapshot_id)
    raw_file_counts = Counter(row["collector_name"] for row in file_rows)
    parsed_row_counts = Counter()
    quarantined_rows = Counter()
    scope_skipped_rows = Counter()
    scope_skip_artifacts = Counter()
    for row in file_rows:
        parsed_row_counts[row["collector_name"]] += int(row["parsed_rows"])
    with manifest._transact() as connection:
        error_rows = connection.execute(
            """
            SELECT files.collector_name, row_errors.error_code, COUNT(*) AS count
            FROM row_errors
            JOIN files ON files.file_id = row_errors.file_id
            WHERE files.snapshot_id = ?
            GROUP BY files.collector_name, row_errors.error_code
            ORDER BY files.collector_name, row_errors.error_code
            """,
            (snapshot_id,),
        ).fetchall()
    for row in error_rows:
        quarantined_rows["%s:%s" % (row["collector_name"], row["error_code"])] = int(row["count"])
    for artifact in manifest.list_artifacts(snapshot_id):
        artifact_kind = artifact["artifact_kind"]
        if artifact_kind.startswith("scope_skipped_"):
            collector_name = artifact_kind[len("scope_skipped_") :]
            scope_skipped_rows[collector_name] += int(artifact["row_count"])
            scope_skip_artifacts[collector_name] += 1
    missing_abstract = sum(1 for row in articles if not row.get("abstract", "").strip())
    missing_venue = sum(1 for row in articles if row.get("venue") == "Unknown Venue")
    missing_publisher = sum(1 for row in articles if row.get("publisher") == "Unknown Publisher")
    date_precision_distribution = Counter(
        row.get("publication_date_precision", "day") for row in articles
    )
    return {
        "snapshot_id": snapshot_id,
        "raw_file_counts_by_collector": dict(raw_file_counts),
        "parsed_row_counts_by_collector": dict(parsed_row_counts),
        "quarantine_counts_by_error_code": dict(quarantined_rows),
        "scope_skipped_rows_by_collector": dict(scope_skipped_rows),
        "scope_skip_artifact_count_by_collector": dict(scope_skip_artifacts),
        "duplicate_doi_count": duplicate_doi_count,
        "pubmed_join_count": pubmed_join_count,
        "orphan_notice_count": len(orphan_notices),
        "canonical_article_count": len(articles),
        "canonical_notice_count": len(notices),
        "missing_abstract_rate": round(missing_abstract / max(1, len(articles)), 4),
        "missing_venue_rate": round(missing_venue / max(1, len(articles)), 4),
        "missing_publisher_rate": round(missing_publisher / max(1, len(articles)), 4),
        "date_precision_distribution": dict(date_precision_distribution),
    }

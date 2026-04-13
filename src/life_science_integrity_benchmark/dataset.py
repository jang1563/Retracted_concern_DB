"""Source ingestion, release IO, and benchmark record construction."""

from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .constants import EXCLUDED_WORK_TYPES, INCLUDED_WORK_TYPES, SNAPSHOT_DATE
from .sample_data import SAMPLE_ARTICLES, SAMPLE_NOTICES, SAMPLE_SIGNALS
from .types import (
    ArticleSource,
    BenchmarkRecord,
    ExternalSignalSource,
    NoticeSource,
    SourceProvenance,
)
from .utils import (
    add_months,
    dedupe_preserve_order,
    parse_date,
    slugify,
    read_jsonl,
    write_csv,
    write_json,
    write_jsonl,
)


def bootstrap_sample_sources(output_dir: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    article_path = output_dir / "articles.jsonl"
    notice_path = output_dir / "official_notices.jsonl"
    signal_path = output_dir / "external_signals.jsonl"
    write_jsonl(article_path, SAMPLE_ARTICLES)
    write_jsonl(notice_path, SAMPLE_NOTICES)
    write_jsonl(signal_path, SAMPLE_SIGNALS)
    return {
        "articles": article_path,
        "official_notices": notice_path,
        "external_signals": signal_path,
    }


def load_article_sources(rows: Iterable[dict]) -> List[ArticleSource]:
    return [ArticleSource(**row) for row in rows]


def load_notice_sources(rows: Iterable[dict]) -> List[NoticeSource]:
    return [NoticeSource(**row) for row in rows]


def load_signal_sources(rows: Iterable[dict]) -> List[ExternalSignalSource]:
    return [ExternalSignalSource(**row) for row in rows]


def load_benchmark_records(rows: Iterable[dict]) -> List[BenchmarkRecord]:
    records = []
    for row in rows:
        row = dict(row)
        row["provenance"] = [SourceProvenance(**entry) for entry in row.get("provenance", [])]
        records.append(BenchmarkRecord(**row))
    return records


def load_source_bundle(source_dir: Path):
    articles = load_article_sources(_load_source_rows(source_dir, "articles"))
    notices = load_notice_sources(_load_source_rows(source_dir, "official_notices"))
    signals = load_signal_sources(_load_source_rows(source_dir, "external_signals"))
    return articles, notices, signals


def article_is_in_scope(article: ArticleSource) -> bool:
    year = parse_date(article.publication_date).year
    work_type = article.work_type.lower()
    if work_type in EXCLUDED_WORK_TYPES:
        return False
    if work_type not in INCLUDED_WORK_TYPES:
        return False
    if year < 2000 or year > 2024:
        return False
    if article.is_pubmed_indexed:
        return True
    return article.openalex_life_science_score >= 0.70


def build_benchmark_records(
    articles: List[ArticleSource],
    notices: List[NoticeSource],
    signals: List[ExternalSignalSource],
    snapshot_date: str = SNAPSHOT_DATE,
) -> List[BenchmarkRecord]:
    notices_by_doi = _group_by_doi(notices)
    signals_by_doi = _group_by_doi(signals)
    snapshot = parse_date(snapshot_date)
    task_a_12m_cutoff = add_months(snapshot, -12)
    task_a_36m_cutoff = add_months(snapshot, -36)

    records = []
    for article in articles:
        if not article_is_in_scope(article):
            continue

        article_notices = [
            notice
            for notice in notices_by_doi.get(article.doi, [])
            if parse_date(notice.notice_date) <= snapshot
        ]
        article_signals = [
            signal
            for signal in signals_by_doi.get(article.doi, [])
            if parse_date(signal.signal_date) <= snapshot
        ]
        provenance = _build_provenance(article_notices, article_signals)

        core_tags = dedupe_preserve_order(
            [notice.notice_type for notice in sorted(article_notices, key=lambda item: item.notice_date)]
        )
        extension_tags = dedupe_preserve_order(
            [signal.tag for signal in sorted(article_signals, key=lambda item: item.signal_date)]
        )

        notice_status = _derive_notice_status(core_tags)
        first_notice_date = _min_date([notice.notice_date for notice in article_notices])
        first_signal_date = _min_date([signal.signal_date for signal in article_signals])
        first_event_date = _min_date(
            [value for value in [first_notice_date, first_signal_date] if value]
        )

        publication_date = parse_date(article.publication_date)
        allowed_feature_view = ["task_a_prepublication", "task_b_core_metadata"]
        if article.oa_status == "open":
            allowed_feature_view.append("task_b_open_text")
        if article_signals:
            allowed_feature_view.append("task_b_link_only_signals")
        auto_publish = notice_status != "none_known_at_snapshot"
        curator_review_required = not auto_publish and bool(article_signals)
        if auto_publish:
            allowed_feature_view.append("site_auto_publish")
        elif curator_review_required:
            allowed_feature_view.append("site_curator_review")

        source_names = [entry.source_name for entry in provenance]
        source_urls = [entry.source_url for entry in provenance]
        author_history_cutoff_date = (
            article.author_history_cutoff_date or article.publication_date
        )
        journal_history_cutoff_date = (
            article.journal_history_cutoff_date or article.publication_date
        )

        task_a_date_bucket = getattr(article, "task_a_date_bucket", None) or (
            "noisy_date"
            if getattr(article, "publication_date_precision", "day") == "year_imputed"
            else "primary"
        )
        task_a_primary = task_a_date_bucket == "primary"

        records.append(
            BenchmarkRecord(
                doi=article.doi,
                internal_id=_internal_id(article.doi),
                title=article.title,
                abstract=article.abstract,
                venue=article.venue,
                publisher=article.publisher,
                publication_date=article.publication_date,
                publication_date_precision=getattr(
                    article, "publication_date_precision", "day"
                ),
                publication_year=publication_date.year,
                work_type=article.work_type,
                subfield=article.subfield,
                is_pubmed_indexed=article.is_pubmed_indexed,
                openalex_life_science_score=article.openalex_life_science_score,
                authors=article.authors,
                institutions=article.institutions,
                author_cluster=_author_cluster(article.authors, article.institutions),
                references_count=article.references_count,
                author_history_signal_count=article.author_history_signal_count,
                journal_history_signal_count=article.journal_history_signal_count,
                oa_status=article.oa_status,
                notice_status=notice_status,
                core_tags=core_tags,
                extension_tags=extension_tags,
                first_signal_date=first_signal_date,
                first_notice_date=first_notice_date,
                snapshot_date=snapshot_date,
                provenance=provenance,
                source_names=source_names,
                source_urls=source_urls,
                source_lineage=list(getattr(article, "source_lineage", [])),
                ingest_snapshot_id=getattr(article, "ingest_snapshot_id", ""),
                allowed_feature_view=allowed_feature_view,
                task_a_feature_cutoff_date=article.publication_date,
                author_history_cutoff_date=author_history_cutoff_date,
                journal_history_cutoff_date=journal_history_cutoff_date,
                task_a_date_bucket=task_a_date_bucket,
                auto_publish=auto_publish,
                curator_review_required=curator_review_required,
                public_summary=_build_public_summary(
                    notice_status=notice_status,
                    core_tags=core_tags,
                    extension_tags=extension_tags,
                    first_notice_date=first_notice_date,
                    first_signal_date=first_signal_date,
                ),
                any_signal_or_notice_within_12m=_event_within_months(
                    article.publication_date, first_event_date, 12
                ),
                any_signal_or_notice_within_36m=_event_within_months(
                    article.publication_date, first_event_date, 36
                ),
                eligible_for_task_a_12m=task_a_primary and publication_date <= task_a_12m_cutoff,
                eligible_for_task_a_36m=task_a_primary and publication_date <= task_a_36m_cutoff,
            )
        )

    return sorted(records, key=lambda item: (item.publication_date, item.doi))


def export_release_bundle(records: List[BenchmarkRecord], output_dir: Path) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "benchmark_v1.jsonl"
    csv_path = output_dir / "benchmark_v1.csv"
    summary_path = output_dir / "summary.json"

    write_jsonl(jsonl_path, records)
    write_csv(
        csv_path,
        [_record_to_flat_row(record) for record in records],
        fieldnames=_flat_fieldnames(),
    )
    write_json(summary_path, build_release_summary(records))
    return {"jsonl": jsonl_path, "csv": csv_path, "summary": summary_path}


def build_release_summary(records: List[BenchmarkRecord]) -> dict:
    subfield_counts = {}
    notice_counts = {}
    total_extension = 0
    for record in records:
        subfield_counts[record.subfield] = subfield_counts.get(record.subfield, 0) + 1
        notice_counts[record.notice_status] = notice_counts.get(record.notice_status, 0) + 1
        total_extension += len(record.extension_tags)
    return {
        "snapshot_date": records[0].snapshot_date if records else SNAPSHOT_DATE,
        "record_count": len(records),
        "subfield_counts": subfield_counts,
        "notice_status_counts": notice_counts,
        "total_extension_tags": total_extension,
        "auto_publish_count": sum(1 for record in records if record.auto_publish),
        "curated_review_count": sum(
            1 for record in records if record.curator_review_required
        ),
        "task_a_12m_eligible_count": sum(
            1 for record in records if record.eligible_for_task_a_12m
        ),
        "task_a_36m_eligible_count": sum(
            1 for record in records if record.eligible_for_task_a_36m
        ),
        "task_a_noisy_date_count": sum(
            1 for record in records if record.task_a_date_bucket == "noisy_date"
        ),
    }


def _group_by_doi(items: Iterable) -> Dict[str, list]:
    grouped: Dict[str, list] = {}
    for item in items:
        grouped.setdefault(item.doi, []).append(item)
    return grouped


def _build_provenance(
    notices: List[NoticeSource], signals: List[ExternalSignalSource]
) -> List[SourceProvenance]:
    entries = []
    for notice in sorted(notices, key=lambda item: item.notice_date):
        entries.append(
            SourceProvenance(
                source_name=notice.source_name,
                source_url=notice.source_url,
                event_date=notice.notice_date,
                event_kind="official_notice",
                observed_label=notice.notice_type,
                rights_status=notice.rights_status,
                summary="Official %s notice recorded in %s."
                % (notice.notice_type.replace("_", " "), notice.source_name),
                publicly_visible=True,
            )
        )
    for signal in sorted(signals, key=lambda item: item.signal_date):
        entries.append(
            SourceProvenance(
                source_name=signal.source_name,
                source_url=signal.source_url,
                event_date=signal.signal_date,
                event_kind="external_signal",
                observed_label=signal.tag,
                rights_status=signal.rights_status,
                summary=signal.summary or (
                    "External integrity signal: %s." % signal.tag.replace("_", " ")
                ),
                publicly_visible=False,
            )
        )
    deduped = []
    seen = set()
    for entry in entries:
        key = (
            entry.source_name,
            entry.source_url,
            entry.event_date,
            entry.event_kind,
            entry.observed_label,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _derive_notice_status(core_tags: List[str]) -> str:
    if "retraction" in core_tags:
        return "retracted"
    if any(tag in core_tags for tag in ("expression_of_concern", "major_correction")):
        return "editorial_notice"
    return "none_known_at_snapshot"


def _min_date(values: List[str]) -> Optional[str]:
    if not values:
        return None
    return min(values)


def _internal_id(doi: str) -> str:
    return "lsib-" + slugify(doi)


def _author_cluster(authors: List[str], institutions: List[str]) -> str:
    if not authors:
        return "unknown-cluster"
    head_author = authors[0]
    institution = institutions[0] if institutions else "unknown"
    return slugify(head_author + "-" + institution)


def _event_within_months(publication_date: str, event_date: Optional[str], months: int) -> bool:
    if not event_date:
        return False
    publication = parse_date(publication_date)
    event = parse_date(event_date)
    return publication <= event <= add_months(publication, months)


def _build_public_summary(
    notice_status: str,
    core_tags: List[str],
    extension_tags: List[str],
    first_notice_date: Optional[str],
    first_signal_date: Optional[str],
) -> str:
    if notice_status == "retracted":
        return "Retracted via an official notice recorded on %s." % first_notice_date
    if notice_status == "editorial_notice":
        joined = ", ".join(core_tags)
        return "Official editorial update on %s with tags: %s." % (
            first_notice_date,
            joined,
        )
    if extension_tags:
        return (
            "Public integrity signals were observed on %s. Curator review is required "
            "before public display."
        ) % first_signal_date
    return "No official notice or external signal was known at the snapshot date."


def _record_to_flat_row(record: BenchmarkRecord) -> dict:
    return {
        "doi": record.doi,
        "internal_id": record.internal_id,
        "title": record.title,
        "venue": record.venue,
        "publisher": record.publisher,
        "publication_date": record.publication_date,
        "publication_date_precision": record.publication_date_precision,
        "publication_year": record.publication_year,
        "work_type": record.work_type,
        "subfield": record.subfield,
        "notice_status": record.notice_status,
        "core_tags": ";".join(record.core_tags),
        "extension_tags": ";".join(record.extension_tags),
        "first_signal_date": record.first_signal_date or "",
        "first_notice_date": record.first_notice_date or "",
        "snapshot_date": record.snapshot_date,
        "source_names": ";".join(record.source_names),
        "source_urls": ";".join(record.source_urls),
        "allowed_feature_view": ";".join(record.allowed_feature_view),
        "task_a_feature_cutoff_date": record.task_a_feature_cutoff_date,
        "author_history_cutoff_date": record.author_history_cutoff_date,
        "journal_history_cutoff_date": record.journal_history_cutoff_date,
        "task_a_date_bucket": record.task_a_date_bucket,
        "auto_publish": str(record.auto_publish),
        "curator_review_required": str(record.curator_review_required),
        "any_signal_or_notice_within_12m": str(record.any_signal_or_notice_within_12m),
        "any_signal_or_notice_within_36m": str(record.any_signal_or_notice_within_36m),
        "eligible_for_task_a_12m": str(record.eligible_for_task_a_12m),
        "eligible_for_task_a_36m": str(record.eligible_for_task_a_36m),
        "authors": ";".join(record.authors),
        "institutions": ";".join(record.institutions),
        "author_cluster": record.author_cluster,
        "references_count": record.references_count,
        "author_history_signal_count": record.author_history_signal_count,
        "journal_history_signal_count": record.journal_history_signal_count,
        "oa_status": record.oa_status,
        "public_summary": record.public_summary,
    }


def _flat_fieldnames() -> List[str]:
    return [
        "doi",
        "internal_id",
        "title",
        "venue",
        "publisher",
        "publication_date",
        "publication_date_precision",
        "publication_year",
        "work_type",
        "subfield",
        "notice_status",
        "core_tags",
        "extension_tags",
        "first_signal_date",
        "first_notice_date",
        "snapshot_date",
        "source_names",
        "source_urls",
        "allowed_feature_view",
        "task_a_feature_cutoff_date",
        "author_history_cutoff_date",
        "journal_history_cutoff_date",
        "task_a_date_bucket",
        "auto_publish",
        "curator_review_required",
        "any_signal_or_notice_within_12m",
        "any_signal_or_notice_within_36m",
        "eligible_for_task_a_12m",
        "eligible_for_task_a_36m",
        "authors",
        "institutions",
        "author_cluster",
        "references_count",
        "author_history_signal_count",
        "journal_history_signal_count",
        "oa_status",
        "public_summary",
    ]


def _load_source_rows(source_dir: Path, source_name: str) -> List[dict]:
    source_dir = Path(source_dir)
    legacy_path = source_dir / ("%s.jsonl" % source_name)
    legacy_gz_path = source_dir / ("%s.jsonl.gz" % source_name)
    shard_dir = source_dir / source_name
    if legacy_path.exists():
        return read_jsonl(legacy_path)
    if legacy_gz_path.exists():
        return read_jsonl(legacy_gz_path)
    if shard_dir.exists():
        rows = []
        for path in sorted(shard_dir.glob("*.jsonl.gz")):
            rows.extend(read_jsonl(path))
        return rows
    return []

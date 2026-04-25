"""Local snapshot parser plugins for full-corpus ingest."""

import csv
import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Set, Tuple

from .constants import (
    ALLOWED_NOTICE_TYPES,
    NOTICE_COLLECTOR,
    OPENALEX_COLLECTOR,
    PUBMED_COLLECTOR,
)
from .manifest import ManifestStore
from .utils import (
    coerce_date_with_precision,
    coerce_bool,
    discover_files,
    first_nonempty,
    normalize_doi,
    open_binary,
    open_text,
    restore_inverted_abstract,
)


OPENALEX_WORK_TYPE_MAP = {
    "article": "article",
    "journal_article": "article",
    "journal-article": "article",
    "review": "review",
    "review_article": "review",
    "review-article": "review",
}

OPENALEX_LIFE_SCIENCE_SCORED_TERMS = (
    "biology",
    "medicine",
    "biochemistry",
    "genetics",
    "bioinformatics",
    "ecology",
    "botany",
    "zoology",
    "microbiology",
    "neuroscience",
    "immunology",
    "pharmacology",
    "epidemiology",
)
OPENALEX_LIFE_SCIENCE_PRIMARY_TERMS = (
    "medicine",
    "medical",
    "clinical",
    "biochemistry",
    "genetics",
    "bioinformatics",
    "ecology",
    "botany",
    "zoology",
    "microbiology",
    "neuroscience",
    "immunology",
    "pharmacology",
    "epidemiology",
)


@dataclass
class FileMeta:
    file_id: str
    collector_name: str
    relative_path: str
    absolute_path: Path
    content_sha256: str
    size_bytes: int


class BaseCollector:
    """Contract for local parser plugins."""

    collector_name = ""
    relative_root = ""
    supported_suffixes: Tuple[str, ...] = ()

    def discover_files(self, raw_root: Path, manifest: ManifestStore, snapshot_id: str) -> List[FileMeta]:
        files = []
        by_key = {
            (row["collector_name"], row["relative_path"]): row
            for row in manifest.list_files(snapshot_id, self.collector_name)
        }
        collector_root = Path(raw_root) / self.relative_root
        for path in discover_files(collector_root, self.supported_suffixes):
            relative_path = str(path.relative_to(raw_root))
            row = by_key[(self.collector_name, relative_path)]
            files.append(
                FileMeta(
                    file_id=row["file_id"],
                    collector_name=self.collector_name,
                    relative_path=relative_path,
                    absolute_path=path,
                    content_sha256=row["content_sha256"],
                    size_bytes=row["size_bytes"],
                )
            )
        return sorted(files, key=lambda item: item.relative_path)

    def iter_raw_records(self, file_meta: FileMeta) -> Iterator[Tuple[int, object]]:
        raise NotImplementedError

    def normalize_record(self, raw_record: object, context: dict) -> dict:
        raise NotImplementedError


class OpenAlexBulkCollector(BaseCollector):
    collector_name = OPENALEX_COLLECTOR
    relative_root = "openalex"
    supported_suffixes = (".jsonl", ".jsonl.gz", ".gz")

    def __init__(self):
        self.early_scope_filter = coerce_bool(
            os.environ.get("LSIB_OPENALEX_EARLY_SCOPE_FILTER"), default=False
        )
        self.scope_doi_allowlist = _load_doi_allowlist(
            os.environ.get("LSIB_OPENALEX_SCOPE_DOI_ALLOWLIST")
        )

    def iter_raw_records(self, file_meta: FileMeta) -> Iterator[Tuple[int, object]]:
        with open_text(file_meta.absolute_path, "rt") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                yield line_number, line

    def normalize_record(self, raw_record: object, context: dict) -> dict:
        try:
            row = json.loads(raw_record)
        except json.JSONDecodeError as exc:
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_json",
                error_message=str(exc),
                raw_record=raw_record,
            )

        doi = _extract_openalex_doi(row)
        if not doi:
            return _quarantine(
                line_number=context["line_number"],
                error_code="missing_doi",
                error_message="DOI is required for canonical benchmark input",
                raw_record=row,
            )

        raw_work_type = str(
            first_nonempty(row.get("type"), row.get("type_crossref"), "article")
        ).strip().lower().replace(" ", "_")
        work_type = OPENALEX_WORK_TYPE_MAP.get(raw_work_type, raw_work_type)
        if work_type not in {"article", "review"}:
            if self.early_scope_filter:
                return _scope_skip(
                    line_number=context["line_number"],
                    reason="unsupported_work_type",
                )
            return _quarantine(
                line_number=context["line_number"],
                error_code="unsupported_work_type",
                error_message="Unsupported work type: %s" % raw_work_type,
                raw_record=row,
            )

        try:
            publication_date, publication_date_precision = coerce_date_with_precision(
                first_nonempty(
                    row.get("publication_date"),
                    row.get("from_publication_date"),
                    row.get("publication_year"),
                )
            )
        except ValueError as exc:
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_date",
                error_message=str(exc),
                raw_record=row,
            )

        authorships = row.get("authorships", [])
        if authorships is None:
            authorships = []
        if not isinstance(authorships, list):
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_authorship_shape",
                error_message="authorships must be a list",
                raw_record=row,
            )
        authors, institutions = _extract_authorships(authorships)
        if authorships and not authors and not institutions:
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_authorship_shape",
                error_message="authorship entries were present but empty after parsing",
                raw_record=row,
            )

        title = (
            first_nonempty(
                row.get("title"),
                row.get("display_name"),
                (row.get("primary_location") or {}).get("landing_page_url"),
                "Untitled work",
            )
            or "Untitled work"
        )
        abstract = (
            first_nonempty(
                row.get("abstract"),
                restore_inverted_abstract(row.get("abstract_inverted_index")),
                "",
            )
            or ""
        )
        try:
            references_count = int(
                first_nonempty(
                    row.get("referenced_works_count"),
                    row.get("references_count"),
                    row.get("cited_by_count"),
                    0,
                )
            )
            openalex_life_science_score = _extract_life_science_score(row)
        except (TypeError, ValueError) as exc:
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_numeric",
                error_message=str(exc),
                raw_record=row,
            )

        scope_skip_reason = self._early_scope_skip_reason(
            doi=doi,
            publication_date=publication_date,
            work_type=work_type,
            life_science_score=openalex_life_science_score,
        )
        if scope_skip_reason:
            return _scope_skip(
                line_number=context["line_number"],
                reason=scope_skip_reason,
            )

        return {
            "kind": "normalized",
            "row": {
                "doi": doi,
                "title": title,
                "abstract": abstract,
                "venue": _extract_venue(row),
                "publisher": _extract_publisher(row),
                "publication_date": publication_date,
                "publication_date_precision": publication_date_precision,
                "work_type": work_type,
                "subfield": _infer_openalex_subfield(title, abstract, row),
                "is_pubmed_indexed": False,
                "openalex_life_science_score": openalex_life_science_score,
                "authors": authors,
                "institutions": institutions,
                "references_count": references_count,
                "author_history_signal_count": 0,
                "journal_history_signal_count": 0,
                "oa_status": _normalize_oa_status(row),
                "pmid": "",
                "mesh_terms": [],
                "keywords": [],
                "pubmed_publication_types": [],
                "pubmed_journal_title": "",
                "author_history_cutoff_date": publication_date,
                "journal_history_cutoff_date": publication_date,
                "source_file_id": context["file_id"],
                "source_line_number": context["line_number"],
                "source_lineage": [
                    {
                        "collector_name": self.collector_name,
                        "source_file_id": context["file_id"],
                        "source_line_number": context["line_number"],
                    }
                ],
                "ingest_snapshot_id": context["snapshot_id"],
                "task_a_date_bucket": _date_bucket(publication_date_precision),
            },
        }

    def _early_scope_skip_reason(
        self,
        doi: str,
        publication_date: str,
        work_type: str,
        life_science_score: float,
    ) -> str:
        if not self.early_scope_filter:
            return ""
        year = int(publication_date[:4])
        if year < 2000:
            return "publication_year_before_2000"
        if year > 2024:
            return "publication_year_after_2024"
        if work_type not in {"article", "review"}:
            return "unsupported_work_type"
        if doi in self.scope_doi_allowlist:
            return ""
        if life_science_score < 0.70:
            return "life_science_score_below_0_70"
        return ""


class LocalNoticeExportCollector(BaseCollector):
    collector_name = NOTICE_COLLECTOR
    relative_root = "official_notices"
    supported_suffixes = (".jsonl", ".jsonl.gz", ".csv", ".csv.gz")

    def iter_raw_records(self, file_meta: FileMeta) -> Iterator[Tuple[int, object]]:
        name = file_meta.absolute_path.name.lower()
        if name.endswith(".csv") or name.endswith(".csv.gz"):
            with open_text(file_meta.absolute_path, "rt") as handle:
                reader = csv.DictReader(handle)
                for line_number, row in enumerate(reader, start=2):
                    yield line_number, dict(row)
            return
        with open_text(file_meta.absolute_path, "rt") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                yield line_number, line

    def normalize_record(self, raw_record: object, context: dict) -> dict:
        if isinstance(raw_record, str):
            try:
                row = json.loads(raw_record)
            except json.JSONDecodeError as exc:
                return _quarantine(
                    line_number=context["line_number"],
                    error_code="bad_json",
                    error_message=str(exc),
                    raw_record=raw_record,
                )
        else:
            row = dict(raw_record)

        try:
            normalized_rows = _extract_notice_rows(row, context, raw_record)
        except ValueError as exc:
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_date",
                error_message=str(exc),
                raw_record=row,
            )
        if normalized_rows:
            if len(normalized_rows) == 1:
                return {"kind": "normalized", "row": normalized_rows[0]}
            return {"kind": "normalized_many", "rows": normalized_rows}

        if _looks_like_notice_row_without_supported_mapping(row):
            return _quarantine(
                line_number=context["line_number"],
                error_code="unknown_notice_type",
                error_message="Could not map notice export row to a supported notice type",
                raw_record=row,
            )
        return _quarantine(
            line_number=context["line_number"],
            error_code="missing_doi",
            error_message="DOI is required for notice matching",
            raw_record=row,
        )


class PubMedIndexCollector(BaseCollector):
    collector_name = PUBMED_COLLECTOR
    relative_root = "pubmed"
    supported_suffixes = (".jsonl", ".jsonl.gz", ".csv", ".csv.gz", ".xml", ".xml.gz")

    def iter_raw_records(self, file_meta: FileMeta) -> Iterator[Tuple[int, object]]:
        name = file_meta.absolute_path.name.lower()
        if name.endswith(".xml") or name.endswith(".xml.gz"):
            yield from self._iter_xml_records(file_meta)
            return
        if name.endswith(".csv") or name.endswith(".csv.gz"):
            with open_text(file_meta.absolute_path, "rt") as handle:
                reader = csv.DictReader(handle)
                for line_number, row in enumerate(reader, start=2):
                    yield line_number, dict(row)
            return
        with open_text(file_meta.absolute_path, "rt") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                yield line_number, line

    def normalize_record(self, raw_record: object, context: dict) -> dict:
        if isinstance(raw_record, ET.Element):
            row = _parse_pubmed_xml_article(raw_record)
        elif isinstance(raw_record, dict) and raw_record.get("__xml_error__"):
            return _quarantine(
                line_number=context["line_number"],
                error_code="bad_xml",
                error_message=raw_record.get("error_message", "Invalid PubMed XML"),
                raw_record=raw_record,
            )
        elif isinstance(raw_record, str):
            try:
                row = json.loads(raw_record)
            except json.JSONDecodeError as exc:
                return _quarantine(
                    line_number=context["line_number"],
                    error_code="bad_json",
                    error_message=str(exc),
                    raw_record=raw_record,
                )
        else:
            row = dict(raw_record)

        doi = normalize_doi(
            first_nonempty(
                row.get("doi"),
                (row.get("article_ids") or {}).get("doi") if isinstance(row.get("article_ids"), dict) else None,
            )
        )
        if not doi:
            return _quarantine(
                line_number=context["line_number"],
                error_code="missing_doi",
                error_message="PubMed join row is missing DOI",
                raw_record=row,
            )

        mesh_terms = _coerce_list(
            first_nonempty(row.get("mesh_terms"), row.get("mesh_headings"), [])
        )
        keywords = _coerce_list(row.get("keywords"))
        publication_types = _coerce_list(
            first_nonempty(row.get("publication_types"), row.get("pub_types"), [])
        )

        return {
            "kind": "normalized",
            "row": {
                "doi": doi,
                "pmid": str(first_nonempty(row.get("pmid"), row.get("pubmed_id"), "")),
                "is_pubmed_indexed": coerce_bool(row.get("is_pubmed_indexed"), default=True),
                "mesh_terms": mesh_terms,
                "keywords": keywords,
                "pubmed_publication_types": publication_types,
                "pubmed_journal_title": str(
                    first_nonempty(
                        row.get("journal_title"),
                        row.get("journal"),
                        "",
                    )
                ),
                "subfield_hint": _infer_pubmed_subfield(mesh_terms, keywords, publication_types),
                "source_file_id": context["file_id"],
                "source_line_number": context["line_number"],
                "ingest_snapshot_id": context["snapshot_id"],
            },
        }

    def _iter_xml_records(self, file_meta: FileMeta) -> Iterator[Tuple[int, object]]:
        with open_binary(file_meta.absolute_path, "rb") as handle:
            article_index = 0
            try:
                for _, element in ET.iterparse(handle, events=("end",)):
                    if element.tag.endswith("PubmedArticle"):
                        article_index += 1
                        yield article_index, element
                        element.clear()
            except ET.ParseError as exc:
                yield 1, {
                    "__xml_error__": True,
                    "error_message": str(exc),
                    "raw_path": str(file_meta.absolute_path),
                }


def get_collector(collector_name: str) -> BaseCollector:
    if collector_name == OPENALEX_COLLECTOR:
        return OpenAlexBulkCollector()
    if collector_name == NOTICE_COLLECTOR:
        return LocalNoticeExportCollector()
    if collector_name == PUBMED_COLLECTOR:
        return PubMedIndexCollector()
    raise KeyError("Unknown collector: %s" % collector_name)


def _quarantine(line_number: int, error_code: str, error_message: str, raw_record: object) -> dict:
    return {
        "kind": "quarantine",
        "row": {
            "line_number": line_number,
            "error_code": error_code,
            "error_message": error_message,
            "raw_excerpt": _raw_excerpt(raw_record),
        },
    }


def _scope_skip(line_number: int, reason: str) -> dict:
    return {
        "kind": "scope_skip",
        "row": {
            "line_number": line_number,
            "reason": reason,
        },
    }


def _load_doi_allowlist(path_value: Optional[str]) -> Set[str]:
    if not path_value:
        return set()
    path = Path(path_value)
    if not path.exists():
        return set()
    return {
        doi
        for line in path.read_text(encoding="utf-8").splitlines()
        if (doi := normalize_doi(line))
    }


def _raw_excerpt(raw_record: object) -> str:
    if isinstance(raw_record, str):
        return raw_record[:500]
    if isinstance(raw_record, ET.Element):
        return ET.tostring(raw_record, encoding="unicode")[:500]
    return json.dumps(raw_record, sort_keys=True)[:500]


def _extract_openalex_doi(row: dict) -> str:
    ids = row.get("ids") or {}
    return normalize_doi(first_nonempty(row.get("doi"), ids.get("doi"), ids.get("pmid_doi")))


def _extract_authorships(authorships: List[dict]) -> Tuple[List[str], List[str]]:
    authors = []
    institutions = []
    for authorship in authorships:
        if not isinstance(authorship, dict):
            continue
        author_name = first_nonempty(
            (authorship.get("author") or {}).get("display_name"),
            authorship.get("author_name"),
            authorship.get("raw_author_name"),
        )
        if author_name:
            authors.append(author_name)
        for institution in authorship.get("institutions", []) or []:
            if not isinstance(institution, dict):
                continue
            name = first_nonempty(
                institution.get("display_name"),
                institution.get("name"),
            )
            if name:
                institutions.append(name)
        for raw_affiliation in authorship.get("raw_affiliation_strings", []) or []:
            if raw_affiliation:
                institutions.append(str(raw_affiliation))
    return authors, sorted(set(institutions))


def _extract_venue(row: dict) -> str:
    host_venue = row.get("host_venue") or {}
    primary_location = row.get("primary_location") or {}
    primary_source = primary_location.get("source") or {}
    locations = row.get("locations") or []
    location_source_names = [
        first_nonempty((location.get("source") or {}).get("display_name"))
        for location in locations
        if isinstance(location, dict)
    ]
    return (
        first_nonempty(
            host_venue.get("display_name"),
            primary_source.get("display_name"),
            primary_location.get("source_display_name"),
            row.get("venue"),
            *location_source_names,
            "Unknown Venue",
        )
        or "Unknown Venue"
    )


def _extract_publisher(row: dict) -> str:
    primary_location = row.get("primary_location") or {}
    primary_source = primary_location.get("source") or {}
    locations = row.get("locations") or []
    location_publishers = [
        first_nonempty(
            (location.get("source") or {}).get("host_organization_name"),
            (location.get("source") or {}).get("display_name"),
        )
        for location in locations
        if isinstance(location, dict)
    ]
    return (
        first_nonempty(
            row.get("publisher"),
            primary_source.get("host_organization_name"),
            primary_source.get("display_name"),
            *location_publishers,
            "Unknown Publisher",
        )
        or "Unknown Publisher"
    )


def _extract_life_science_score(row: dict) -> float:
    explicit = row.get("openalex_life_science_score")
    if explicit is not None:
        return round(float(explicit), 4)

    best = 0.0
    for concept in row.get("concepts", []) or []:
        if not isinstance(concept, dict):
            continue
        name = (concept.get("display_name") or "").lower()
        if _matches_any([name], OPENALEX_LIFE_SCIENCE_SCORED_TERMS):
            best = max(best, float(concept.get("score", 0.0)))

    primary_topic = row.get("primary_topic") or {}
    primary_topic_names = [
        ((primary_topic.get("subfield") or {}).get("display_name") or "").lower(),
        ((primary_topic.get("field") or {}).get("display_name") or "").lower(),
    ]
    if _matches_any(primary_topic_names, OPENALEX_LIFE_SCIENCE_PRIMARY_TERMS):
        best = max(best, 0.90)

    for topic in row.get("topics", []) or []:
        if not isinstance(topic, dict):
            continue
        names = [
            (topic.get("display_name") or "").lower(),
            ((topic.get("subfield") or {}).get("display_name") or "").lower(),
            ((topic.get("field") or {}).get("display_name") or "").lower(),
        ]
        if _matches_any(names, OPENALEX_LIFE_SCIENCE_SCORED_TERMS):
            score = topic.get("score")
            if score is None:
                if _matches_any(names, OPENALEX_LIFE_SCIENCE_PRIMARY_TERMS):
                    best = max(best, 0.90)
            else:
                best = max(best, float(score))
    return round(best, 4)


def _matches_any(names: List[str], terms: Tuple[str, ...]) -> bool:
    return any(token in name for name in names for token in terms)


def _infer_openalex_subfield(title: str, abstract: str, row: dict) -> str:
    primary_topic = row.get("primary_topic") or {}
    explicit = first_nonempty(
        row.get("subfield"),
        (primary_topic.get("subfield") or {}).get("display_name"),
    )
    if explicit:
        explicit = str(explicit).lower()
        if "bioinformatic" in explicit or "computational" in explicit or "genomic" in explicit:
            return "bioinformatics"
        if any(token in explicit for token in ("medicine", "clinical", "oncology", "biomedical", "pathology")):
            return "biomedicine"
        if any(token in explicit for token in ("biology", "ecology", "botany", "zoology", "microbiology")):
            return "biology"

    concept_names = " ".join(
        (concept.get("display_name") or "")
        for concept in row.get("concepts", []) or []
        if isinstance(concept, dict)
    ).lower()
    topic_names = " ".join(
        (topic.get("display_name") or "")
        for topic in row.get("topics", []) or []
        if isinstance(topic, dict)
    ).lower()
    text = " ".join([title, abstract, concept_names, topic_names]).lower()
    if any(token in text for token in ("bioinformatic", "genome", "transcript", "metagenom", "variant", "pipeline")):
        return "bioinformatics"
    if any(token in text for token in ("clinical", "cohort", "hospital", "biomarker", "patient", "glioma", "sepsis", "fibrosis")):
        return "biomedicine"
    return "biology"


def _normalize_oa_status(row: dict) -> str:
    oa_status = row.get("oa_status")
    if oa_status:
        return str(oa_status).lower()
    open_access = row.get("open_access") or {}
    if open_access.get("oa_status"):
        return str(open_access["oa_status"]).lower()
    if row.get("is_oa") is True or open_access.get("is_oa") is True:
        return "open"
    if row.get("is_oa") is False or open_access.get("is_oa") is False:
        return "closed"
    return "unknown"


def _date_bucket(publication_date_precision: str) -> str:
    if publication_date_precision == "year_imputed":
        return "noisy_date"
    return "primary"


def _normalize_notice_type(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"correction", "corrigendum", "erratum"}:
        return "major_correction"
    if normalized in {"partial_retraction", "withdrawal", "removal"}:
        return "retraction"
    return normalized


def normalize_notice_label(value: Optional[str]) -> str:
    return _normalize_notice_type(value)


def extract_notice_rows_for_export(
    row: dict,
    snapshot_id: str,
    file_id: str,
    line_number: int,
    source_name_override: Optional[str] = None,
    rights_status_override: Optional[str] = None,
) -> List[dict]:
    export_row = dict(row)
    if source_name_override:
        export_row["source_name"] = source_name_override
    if rights_status_override:
        export_row["rights_status"] = rights_status_override
    return _extract_notice_rows(
        export_row,
        context={
            "snapshot_id": snapshot_id,
            "file_id": file_id,
            "line_number": line_number,
        },
        raw_record=export_row,
    )


def _extract_notice_rows(row: dict, context: dict, raw_record: object) -> List[dict]:
    source_name = _notice_source_name(row)
    rights_status = first_nonempty(row.get("rights_status"), row.get("rights"), "metadata_only")
    explicit_rows = _extract_explicit_notice_rows(row, context, source_name, rights_status, raw_record)
    relation_rows = _extract_crossref_relation_notice_rows(
        row, context, source_name, rights_status, raw_record
    )
    update_rows = _extract_crossmark_update_notice_rows(
        row, context, source_name, rights_status, raw_record
    )
    seen = set()
    deduped = []
    for item in explicit_rows + relation_rows + update_rows:
        key = (
            item["doi"],
            item["notice_type"],
            item["notice_date"],
            item["source_name"],
            item["source_url"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_explicit_notice_rows(
    row: dict, context: dict, source_name: str, rights_status: str, raw_record: object
) -> List[dict]:
    doi = normalize_doi(
        first_nonempty(
            row.get("doi"),
            row.get("DOI"),
            row.get("target_doi"),
            row.get("article_doi"),
            row.get("original_doi"),
            row.get("original_paper_doi"),
            row.get("paper_doi"),
            row.get("record_doi"),
            row.get("object_doi"),
        )
    )
    notice_label = first_nonempty(
        row.get("notice_type"),
        row.get("tag"),
        row.get("notice_label"),
        row.get("update_type"),
        row.get("retraction_nature"),
    )
    if not doi or not notice_label:
        return []
    notice_type = _normalize_notice_type(notice_label)
    if notice_type not in ALLOWED_NOTICE_TYPES:
        return []
    notice_date, notice_date_precision = _extract_notice_date(row)
    if not notice_date:
        raise _notice_bad_date(raw_record)
    return [
        _notice_row(
            doi=doi,
            notice_type=notice_type,
            notice_date=notice_date,
            notice_date_precision=notice_date_precision,
            source_name=source_name,
            source_url=_extract_notice_source_url(row, fallback_doi=""),
            rights_status=rights_status,
            context=context,
        )
    ]


def _extract_crossref_relation_notice_rows(
    row: dict, context: dict, source_name: str, rights_status: str, raw_record: object
) -> List[dict]:
    relation = row.get("relation")
    if not isinstance(relation, dict):
        return []
    row_type_fallback = _normalize_notice_type(
        first_nonempty(row.get("type"), row.get("Type"), row.get("subtype"))
    )
    notice_date, notice_date_precision = _extract_notice_date(row)
    if not notice_date:
        raise _notice_bad_date(raw_record)
    source_url = _extract_notice_source_url(
        row,
        fallback_doi=normalize_doi(first_nonempty(row.get("DOI"), row.get("doi"))),
    )
    extracted = []
    for key, value in relation.items():
        notice_type = _notice_type_from_relation_key(key, row_type_fallback=row_type_fallback)
        if notice_type is None:
            continue
        entries = value if isinstance(value, list) else [value]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            doi = normalize_doi(
                first_nonempty(
                    entry.get("id"),
                    entry.get("DOI"),
                    entry.get("doi"),
                    entry.get("identifier"),
                )
            )
            if not doi:
                continue
            extracted.append(
                _notice_row(
                    doi=doi,
                    notice_type=notice_type,
                    notice_date=notice_date,
                    notice_date_precision=notice_date_precision,
                    source_name=source_name,
                    source_url=source_url,
                    rights_status=rights_status,
                    context=context,
                )
            )
    return extracted


def _extract_crossmark_update_notice_rows(
    row: dict, context: dict, source_name: str, rights_status: str, raw_record: object
) -> List[dict]:
    target_doi = normalize_doi(first_nonempty(row.get("doi"), row.get("DOI")))
    if not target_doi:
        return []
    update_lists = []
    for key in ("update-to", "update_to", "updates", "crossmark_updates"):
        value = row.get(key)
        if isinstance(value, list):
            update_lists.extend(value)
    if not update_lists:
        return []
    extracted = []
    for entry in update_lists:
        if not isinstance(entry, dict):
            continue
        notice_label = first_nonempty(
            entry.get("label"),
            entry.get("type"),
            entry.get("update_type"),
            entry.get("notice_type"),
        )
        notice_type = _normalize_notice_type(notice_label)
        if notice_type not in ALLOWED_NOTICE_TYPES:
            continue
        notice_date, notice_date_precision = _extract_notice_date(entry, row=row)
        if not notice_date:
            raise _notice_bad_date(raw_record)
        source_url = _extract_notice_source_url(
            entry,
            fallback_doi=normalize_doi(first_nonempty(entry.get("DOI"), entry.get("doi"))),
        )
        if not source_url:
            source_url = _extract_notice_source_url(
                row,
                fallback_doi=normalize_doi(first_nonempty(entry.get("DOI"), entry.get("doi"))),
            )
        extracted.append(
            _notice_row(
                doi=target_doi,
                notice_type=notice_type,
                notice_date=notice_date,
                notice_date_precision=notice_date_precision,
                source_name=source_name,
                source_url=source_url,
                rights_status=rights_status,
                context=context,
            )
        )
    return extracted


def _notice_row(
    doi: str,
    notice_type: str,
    notice_date: str,
    notice_date_precision: str,
    source_name: str,
    source_url: str,
    rights_status: str,
    context: dict,
) -> dict:
    return {
        "doi": doi,
        "notice_type": notice_type,
        "notice_date": notice_date,
        "notice_date_precision": notice_date_precision,
        "source_name": source_name,
        "source_url": source_url,
        "rights_status": rights_status,
        "source_file_id": context["file_id"],
        "source_line_number": context["line_number"],
        "ingest_snapshot_id": context["snapshot_id"],
    }


def _extract_notice_date(*rows, row=None) -> Tuple[str, str]:
    candidates = list(rows)
    if row is not None:
        candidates.append(row)
    for candidate in candidates:
        date_value = first_nonempty(
            candidate.get("notice_date"),
            candidate.get("date"),
            candidate.get("updated"),
            candidate.get("update_date"),
            _extract_date_object(candidate.get("issued")),
            _extract_date_object(candidate.get("published")),
            _extract_date_object(candidate.get("published-print")),
            _extract_date_object(candidate.get("published-online")),
            _extract_date_object(candidate.get("deposited")),
            _extract_date_object(candidate.get("created")),
            _extract_date_object(candidate.get("indexed")),
            _extract_date_object(candidate.get("posted")),
        )
        if date_value:
            try:
                return coerce_date_with_precision(date_value)
            except ValueError:
                continue
    return "", ""


def _extract_date_object(value):
    if isinstance(value, dict):
        parts = value.get("date-parts") or value.get("date_parts")
        if isinstance(parts, list) and parts:
            head = parts[0]
            if isinstance(head, list):
                items = [str(item) for item in head if item is not None]
            else:
                items = [str(item) for item in parts if item is not None]
            if len(items) >= 3:
                return "%s-%s-%s" % (
                    items[0].zfill(4),
                    items[1].zfill(2),
                    items[2].zfill(2),
                )
            if len(items) == 2:
                return "%s-%s" % (items[0].zfill(4), items[1].zfill(2))
            if len(items) == 1:
                return items[0].zfill(4)
    return value


def _extract_notice_source_url(row: dict, fallback_doi: str) -> str:
    url = first_nonempty(
        row.get("source_url"),
        row.get("url"),
        row.get("URL"),
        ((row.get("resource") or {}).get("primary") or {}).get("URL")
        if isinstance(row.get("resource"), dict)
        else None,
    )
    if url:
        return str(url)
    if fallback_doi:
        return "https://doi.org/%s" % fallback_doi
    return ""


def _notice_source_name(row: dict) -> str:
    explicit = first_nonempty(row.get("source_name"), row.get("source"))
    if explicit:
        return str(explicit)
    if any(key in row for key in ("relation", "update-to", "update_to", "DOI", "URL")):
        return "Crossref/Crossmark Export"
    return "Official Notice Export"


def _notice_type_from_relation_key(key: str, row_type_fallback: str = "") -> Optional[str]:
    normalized = str(key).strip().lower().replace("-", "_").replace(" ", "_")
    if "retraction" in normalized:
        return "retraction"
    if "concern" in normalized:
        return "expression_of_concern"
    if "update" in normalized and row_type_fallback in ALLOWED_NOTICE_TYPES:
        return row_type_fallback
    if any(token in normalized for token in ("correction", "corrected", "corrigendum", "erratum", "update")):
        return "major_correction"
    return None


def _looks_like_notice_row_without_supported_mapping(row: dict) -> bool:
    if any(key in row for key in ("notice_type", "tag", "notice_label", "update-to", "update_to", "updates", "relation")):
        return True
    if str(first_nonempty(row.get("type"), row.get("Type"), "")).lower() in {
        "retraction",
        "correction",
        "expression_of_concern",
    }:
        return True
    return False


def _notice_bad_date(raw_record: object) -> ValueError:
    return ValueError("Could not coerce notice date from row: %s" % _raw_excerpt(raw_record))


def _coerce_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    separator = "|" if "|" in text else ";"
    if separator in text:
        return [item.strip() for item in text.split(separator) if item.strip()]
    return [text]


def _infer_pubmed_subfield(mesh_terms: List[str], keywords: List[str], publication_types: List[str]) -> str:
    text = " ".join(mesh_terms + keywords + publication_types).lower()
    if any(token in text for token in ("genomics", "sequence analysis", "bioinformatics", "computational biology", "transcriptome", "proteome")):
        return "bioinformatics"
    if any(token in text for token in ("clinical", "patients", "humans", "disease", "biomarkers", "neoplasms", "therapy")):
        return "biomedicine"
    return "biology"


def _parse_pubmed_xml_article(element: ET.Element) -> dict:
    if isinstance(element, dict) and element.get("__xml_error__"):
        return element
    pmid = _find_text(element, "./MedlineCitation/PMID")
    doi = _find_pubmed_doi(element)
    mesh_terms = [
        descriptor.text.strip()
        for descriptor in element.findall("./MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")
        if descriptor.text and descriptor.text.strip()
    ]
    keywords = [
        keyword.text.strip()
        for keyword in element.findall(".//KeywordList/Keyword")
        if keyword.text and keyword.text.strip()
    ]
    publication_types = [
        item.text.strip()
        for item in element.findall("./MedlineCitation/Article/PublicationTypeList/PublicationType")
        if item.text and item.text.strip()
    ]
    journal_title = first_nonempty(
        _find_text(element, "./MedlineCitation/Article/Journal/Title"),
        _find_text(element, "./MedlineCitation/Article/Journal/ISOAbbreviation"),
        "",
    )
    return {
        "pmid": pmid or "",
        "doi": doi or "",
        "mesh_terms": mesh_terms,
        "keywords": keywords,
        "publication_types": publication_types,
        "journal_title": journal_title or "",
        "is_pubmed_indexed": True,
    }


def _find_pubmed_doi(element: ET.Element) -> str:
    for article_id in element.findall("./PubmedData/ArticleIdList/ArticleId"):
        if (article_id.attrib.get("IdType") or "").lower() == "doi" and article_id.text:
            return article_id.text.strip()
    for location_id in element.findall("./MedlineCitation/Article/ELocationID"):
        if (location_id.attrib.get("EIdType") or "").lower() == "doi" and location_id.text:
            return location_id.text.strip()
    return ""


def _find_text(element: ET.Element, xpath: str) -> str:
    found = element.find(xpath)
    if found is None or not found.text:
        return ""
    return found.text.strip()

"""Core data types used throughout the benchmark pipeline."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ArticleSource:
    doi: str
    title: str
    abstract: str
    venue: str
    publisher: str
    publication_date: str
    work_type: str
    subfield: str
    is_pubmed_indexed: bool
    openalex_life_science_score: float
    authors: List[str]
    institutions: List[str]
    references_count: int
    author_history_signal_count: int
    journal_history_signal_count: int
    oa_status: str
    publication_date_precision: str = "day"
    pmid: str = ""
    mesh_terms: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    pubmed_publication_types: List[str] = field(default_factory=list)
    pubmed_journal_title: str = ""
    author_history_cutoff_date: Optional[str] = None
    journal_history_cutoff_date: Optional[str] = None
    source_file_id: str = ""
    source_line_number: int = 0
    source_lineage: List[dict] = field(default_factory=list)
    ingest_snapshot_id: str = ""
    task_a_date_bucket: str = "primary"


@dataclass
class NoticeSource:
    doi: str
    notice_type: str
    notice_date: str
    source_name: str
    source_url: str
    notice_date_precision: str = "day"
    rights_status: str = "metadata_only"
    source_file_id: str = ""
    source_line_number: int = 0
    ingest_snapshot_id: str = ""


@dataclass
class ExternalSignalSource:
    doi: str
    tag: str
    signal_date: str
    source_name: str
    source_url: str
    rights_status: str = "link_only"
    curator_required: bool = True
    summary: str = ""


@dataclass
class SourceProvenance:
    source_name: str
    source_url: str
    event_date: str
    event_kind: str
    observed_label: str
    rights_status: str
    summary: str
    publicly_visible: bool


@dataclass
class BenchmarkRecord:
    doi: str
    internal_id: str
    title: str
    abstract: str
    venue: str
    publisher: str
    publication_date: str
    publication_year: int
    work_type: str
    subfield: str
    is_pubmed_indexed: bool
    openalex_life_science_score: float
    authors: List[str]
    institutions: List[str]
    author_cluster: str
    references_count: int
    author_history_signal_count: int
    journal_history_signal_count: int
    oa_status: str
    notice_status: str
    publication_date_precision: str = "day"
    core_tags: List[str] = field(default_factory=list)
    extension_tags: List[str] = field(default_factory=list)
    first_signal_date: Optional[str] = None
    first_notice_date: Optional[str] = None
    snapshot_date: str = ""
    provenance: List[SourceProvenance] = field(default_factory=list)
    source_names: List[str] = field(default_factory=list)
    source_urls: List[str] = field(default_factory=list)
    source_lineage: List[dict] = field(default_factory=list)
    ingest_snapshot_id: str = ""
    allowed_feature_view: List[str] = field(default_factory=list)
    task_a_feature_cutoff_date: str = ""
    author_history_cutoff_date: str = ""
    journal_history_cutoff_date: str = ""
    task_a_date_bucket: str = "primary"
    auto_publish: bool = False
    curator_review_required: bool = False
    public_summary: str = ""
    any_signal_or_notice_within_12m: bool = False
    any_signal_or_notice_within_36m: bool = False
    eligible_for_task_a_12m: bool = False
    eligible_for_task_a_36m: bool = False


@dataclass
class SplitManifest:
    task_name: str
    split_kind: str
    train_dois: List[str]
    val_dois: List[str]
    test_dois: List[str]
    group_field: Optional[str] = None
    holdout_values: List[str] = field(default_factory=list)


@dataclass
class AuditReport:
    snapshot_date: str
    task_a_feature_fields: List[str]
    banned_fields_checked: List[str]
    leaked_fields_found: List[str]
    records_checked: int
    records_with_invalid_event_order: List[str]
    records_with_snapshot_violations: List[str]
    records_missing_feature_provenance: List[str]
    feature_cutoff_violations: List[Dict[str, str]]
    passed: bool


@dataclass
class BaselineRun:
    model_name: str
    task_name: str
    backend_used: str
    metrics: dict

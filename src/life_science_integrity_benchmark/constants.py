"""Shared constants for the benchmark implementation."""

SNAPSHOT_DATE = "2026-04-09"
PARSER_BUNDLE_VERSION = "2026-04-10-v2"
MANIFEST_DB_PATH = "data/manifests/ingest.sqlite3"
CANONICAL_SHARD_SIZE = 100000

OPENALEX_COLLECTOR = "openalex_bulk"
NOTICE_COLLECTOR = "local_notice_export"
PUBMED_COLLECTOR = "pubmed_index"
RESERVED_COLLECTORS = ["external_signals_generic"]
SOURCE_FAMILY_OPENALEX_NOTICES = "openalex_notices"

INCLUDED_WORK_TYPES = {"article", "review"}
EXCLUDED_WORK_TYPES = {
    "conference_paper",
    "editorial",
    "letter",
    "book_chapter",
    "correction",
    "notice",
    "protocol",
    "preprint",
}

TASK_A_FEATURE_FIELDS = [
    "title",
    "abstract",
    "venue",
    "publisher",
    "publication_year",
    "subfield",
    "is_pubmed_indexed",
    "openalex_life_science_score",
    "references_count",
    "author_history_signal_count",
    "journal_history_signal_count",
    "oa_status",
    "authors",
    "institutions",
]

TASK_A_CENSORED_FIELDS = [
    "author_history_signal_count",
    "journal_history_signal_count",
]

BANNED_TASK_A_FIELDS = [
    "notice_status",
    "core_tags",
    "extension_tags",
    "first_signal_date",
    "first_notice_date",
    "source_names",
    "source_urls",
    "allowed_feature_view",
    "public_summary",
    "auto_publish",
    "curator_review_required",
]

GROUP_HOLDOUT_FIELDS = [
    "author_cluster",
    "venue",
    "publisher",
]

DATE_PRECISION_ORDER = {
    "year_imputed": 0,
    "month_imputed": 1,
    "day": 2,
}
PRIMARY_TASK_A_DATE_BUCKETS = {"primary"}
NOISY_TASK_A_DATE_BUCKETS = {"noisy_date"}
ALLOWED_NOTICE_TYPES = {
    "retraction",
    "expression_of_concern",
    "major_correction",
}
OPENALEX_QUARANTINE_CODES = {
    "missing_doi",
    "bad_json",
    "bad_date",
    "unsupported_work_type",
    "bad_authorship_shape",
}
NOTICE_QUARANTINE_CODES = {
    "missing_doi",
    "bad_date",
    "unknown_notice_type",
    "bad_csv",
    "bad_json",
}
PUBMED_QUARANTINE_CODES = {
    "missing_doi",
    "bad_date",
    "bad_json",
    "bad_csv",
    "bad_xml",
}

SITE_DISCLAIMER = (
    "This page summarizes public integrity signals and notices. "
    "It is not a determination of misconduct."
)

POLICY_CONTACT = "integrity-review@example.org"

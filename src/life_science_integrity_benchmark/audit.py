"""Leakage checks for Task A early-warning evaluation."""

from typing import List

from .constants import BANNED_TASK_A_FIELDS, TASK_A_CENSORED_FIELDS, TASK_A_FEATURE_FIELDS
from .types import AuditReport, BenchmarkRecord
from .utils import parse_date


def build_leakage_report(records: List[BenchmarkRecord], snapshot_date: str) -> AuditReport:
    leaked_fields_found = [
        field for field in TASK_A_FEATURE_FIELDS if field in BANNED_TASK_A_FIELDS
    ]
    invalid_event_order = []
    snapshot_violations = []
    missing_feature_provenance = []
    feature_cutoff_violations = []
    snapshot = parse_date(snapshot_date)

    for record in records:
        publication = parse_date(record.publication_date)
        if record.snapshot_date and record.snapshot_date != snapshot_date:
            snapshot_violations.append(record.doi)
        if record.first_signal_date and parse_date(record.first_signal_date) < publication:
            invalid_event_order.append(record.doi)
        if record.first_notice_date and parse_date(record.first_notice_date) < publication:
            invalid_event_order.append(record.doi)
        if record.first_signal_date and parse_date(record.first_signal_date) > snapshot:
            snapshot_violations.append(record.doi)
        if record.first_notice_date and parse_date(record.first_notice_date) > snapshot:
            snapshot_violations.append(record.doi)

        if not record.task_a_feature_cutoff_date:
            missing_feature_provenance.append(record.doi)
        if not record.author_history_cutoff_date and "author_history_signal_count" in TASK_A_CENSORED_FIELDS:
            missing_feature_provenance.append(record.doi)
        if not record.journal_history_cutoff_date and "journal_history_signal_count" in TASK_A_CENSORED_FIELDS:
            missing_feature_provenance.append(record.doi)

        if record.task_a_feature_cutoff_date:
            task_a_cutoff = parse_date(record.task_a_feature_cutoff_date)
            if task_a_cutoff != publication:
                feature_cutoff_violations.append(
                    {
                        "doi": record.doi,
                        "field": "task_a_feature_cutoff_date",
                        "cutoff_date": record.task_a_feature_cutoff_date,
                        "publication_date": record.publication_date,
                    }
                )
        if record.author_history_cutoff_date:
            author_cutoff = parse_date(record.author_history_cutoff_date)
            if author_cutoff > publication:
                feature_cutoff_violations.append(
                    {
                        "doi": record.doi,
                        "field": "author_history_signal_count",
                        "cutoff_date": record.author_history_cutoff_date,
                        "publication_date": record.publication_date,
                    }
                )
        if record.journal_history_cutoff_date:
            journal_cutoff = parse_date(record.journal_history_cutoff_date)
            if journal_cutoff > publication:
                feature_cutoff_violations.append(
                    {
                        "doi": record.doi,
                        "field": "journal_history_signal_count",
                        "cutoff_date": record.journal_history_cutoff_date,
                        "publication_date": record.publication_date,
                    }
                )

    return AuditReport(
        snapshot_date=snapshot_date,
        task_a_feature_fields=TASK_A_FEATURE_FIELDS,
        banned_fields_checked=BANNED_TASK_A_FIELDS,
        leaked_fields_found=leaked_fields_found,
        records_checked=len(records),
        records_with_invalid_event_order=sorted(set(invalid_event_order)),
        records_with_snapshot_violations=sorted(set(snapshot_violations)),
        records_missing_feature_provenance=sorted(set(missing_feature_provenance)),
        feature_cutoff_violations=feature_cutoff_violations,
        passed=not leaked_fields_found
        and not invalid_event_order
        and not snapshot_violations
        and not missing_feature_provenance
        and not feature_cutoff_violations,
    )

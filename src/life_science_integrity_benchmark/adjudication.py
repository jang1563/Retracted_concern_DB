"""Adjudication queue builder for double review workflows."""

from collections import defaultdict
from typing import List

from .types import BenchmarkRecord
from .utils import write_csv, write_json


def build_adjudication_rows(records: List[BenchmarkRecord], sample_size: int = 500) -> List[dict]:
    grouped = defaultdict(list)
    for record in records:
        key = (record.notice_status, record.subfield)
        grouped[key].append(record)

    rows = []
    keys = sorted(grouped)
    while len(rows) < sample_size and any(grouped.values()):
        for key in keys:
            if not grouped[key] or len(rows) >= sample_size:
                continue
            record = grouped[key].pop(0)
            rows.append(
                {
                    "doi": record.doi,
                    "title": record.title,
                    "publication_date": record.publication_date,
                    "subfield": record.subfield,
                    "notice_status": record.notice_status,
                    "core_tags": ";".join(record.core_tags),
                    "extension_tags": ";".join(record.extension_tags),
                    "public_release_eligibility": _public_release_eligibility(record),
                    "reviewer_a_decision": "",
                    "reviewer_b_decision": "",
                    "consensus_decision": "",
                    "wording_review": "",
                    "notes": "",
                }
            )
    return rows


def _public_release_eligibility(record: BenchmarkRecord) -> str:
    if record.auto_publish:
        return "official_notice"
    if record.curator_review_required:
        return "curator_review"
    return "none_known_at_snapshot"


def export_adjudication_pack(
    records: List[BenchmarkRecord],
    csv_path,
    summary_path,
    sample_size: int = 500,
    protocol_path=None,
) -> dict:
    rows = build_adjudication_rows(records, sample_size=sample_size)
    fieldnames = list(rows[0].keys()) if rows else [
        "doi",
        "title",
        "publication_date",
        "subfield",
        "notice_status",
        "core_tags",
        "extension_tags",
        "public_release_eligibility",
        "reviewer_a_decision",
        "reviewer_b_decision",
        "consensus_decision",
        "wording_review",
        "notes",
    ]
    write_csv(csv_path, rows, fieldnames)
    summary_payload = {
        "requested_sample_size": sample_size,
        "rows_written": len(rows),
        "coverage_complete": len(rows) >= sample_size,
        "strata_counts": _strata_counts(rows),
    }
    write_json(
        summary_path,
        summary_payload,
    )
    result = {"csv": csv_path, "summary": summary_path}
    if protocol_path is not None:
        protocol_path.write_text(_protocol_markdown(summary_payload), encoding="utf-8")
        result["protocol"] = protocol_path
    return result


def _strata_counts(rows: List[dict]) -> dict:
    output = {}
    for row in rows:
        key = "%s|%s" % (row["notice_status"], row["subfield"])
        output[key] = output.get(key, 0) + 1
    return output


def _protocol_markdown(summary_payload: dict) -> str:
    return """# Adjudication Protocol

## Goal

Create a doubly reviewed subset for validating weak labels, tag quality, and public-site wording.

## Reviewer Workflow

1. Review the paper metadata and linked provenance.
2. Decide whether the weak label is directionally correct.
3. Record whether the public wording is factual, cautious, and source-linked.
4. Escalate disagreements to consensus review.

## Required Fields

- `reviewer_a_decision`
- `reviewer_b_decision`
- `consensus_decision`
- `wording_review`
- `notes`

## Public Release Eligibility Labels

- `official_notice`: official notice metadata may be public
- `curator_review`: non-notice external signals require curator review before public display
- `none_known_at_snapshot`: background/negative record with no public integrity signal at the snapshot

## Current Pack Summary

- Requested rows: {requested}
- Written rows: {written}
- Coverage complete: {coverage}
""".format(
        requested=summary_payload["requested_sample_size"],
        written=summary_payload["rows_written"],
        coverage=summary_payload["coverage_complete"],
    )

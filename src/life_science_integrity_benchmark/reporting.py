"""Experiment report builders for benchmark runs."""

from pathlib import Path
from typing import Dict

from .utils import write_json


def build_experiment_report(
    summary: dict,
    splits: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_b_baseline: dict,
    markdown_path: Path,
    json_path: Path,
    ingest_summary: dict = None,
) -> Dict[str, Path]:
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(
        _markdown_report(
            summary,
            splits,
            leakage_report,
            task_a_baselines,
            task_b_baseline,
            ingest_summary=ingest_summary or {},
        ),
        encoding="utf-8",
    )
    write_json(
        json_path,
        {
            "summary": summary,
            "splits": splits,
            "leakage_report": leakage_report,
            "task_a_baselines": task_a_baselines,
            "task_b_baseline": task_b_baseline,
            "ingest_summary": ingest_summary or {},
        },
    )
    return {"markdown": markdown_path, "json": json_path}


def _markdown_report(
    summary: dict,
    splits: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_b_baseline: dict,
    ingest_summary: dict,
) -> str:
    task_a_lines = []
    for task_name, runs in sorted(task_a_baselines.items()):
        task_a_lines.append("### %s" % task_name)
        for run in runs:
            metrics = run["metrics"]
            task_a_lines.append(
                "- `%s` (`%s`): AUPRC=%s, Recall@1pct=%s, Recall@5pct=%s, ECE=%s"
                % (
                    run["model_name"],
                    run["backend_used"],
                    metrics.get("AUPRC"),
                    metrics.get("Recall@1pct"),
                    metrics.get("Recall@5pct"),
                    metrics.get("ECE"),
                )
            )
    split_lines = []
    for name, manifest in sorted(splits.items()):
        split_lines.append(
            "- `%s`: kind=%s, train=%s, val=%s, test=%s"
            % (
                name,
                manifest.get("split_kind"),
                len(manifest.get("train_dois", [])),
                len(manifest.get("val_dois", [])),
                len(manifest.get("test_dois", [])),
            )
        )
    leak_status = "PASS" if leakage_report.get("passed") else "FAIL"
    task_b_metrics = task_b_baseline.get("metrics", {})
    ingest_lines = []
    if ingest_summary:
        ingest_lines.extend(
            [
                "- Snapshot ID: `%s`" % ingest_summary.get("snapshot_id"),
                "- Raw files by collector: `%s`"
                % ingest_summary.get("raw_file_counts_by_collector"),
                "- Parsed rows by collector: `%s`"
                % ingest_summary.get("parsed_row_counts_by_collector"),
                "- Quarantine counts: `%s`"
                % ingest_summary.get("quarantine_counts_by_error_code"),
                "- Duplicate DOI count: `%s`"
                % ingest_summary.get("duplicate_doi_count"),
                "- Orphan notice count: `%s`"
                % ingest_summary.get("orphan_notice_count"),
                "- Date precision distribution: `%s`"
                % ingest_summary.get("date_precision_distribution"),
            ]
        )
    return """# Benchmark Experiment Report

## Snapshot Summary

- Snapshot date: `{snapshot}`
- Records: `{records}`
- Public records: `{public_records}`
- Curator-review records: `{curator_records}`
- Task A 12m eligible: `{eligible_12m}`
- Task A 36m eligible: `{eligible_36m}`

## Leakage Audit

- Status: **{leak_status}**
- Invalid event ordering: `{invalid_event_count}`
- Snapshot violations: `{snapshot_violations}`
- Feature cutoff violations: `{feature_cutoff_violations}`

## Ingest Summary

{ingest_lines}

## Splits

{split_lines}

## Task A Baselines

{task_a_lines}

## Task B Baseline

- `{model_name}` (`{backend}`): notice accuracy={notice_accuracy}, tag macro-F1={tag_macro_f1}, provenance coverage={coverage}

## Notes

- This report summarizes benchmark artifacts from a single frozen release bundle.
- Group holdout manifests are included for author clusters, venues, and publishers to support robustness analysis.
""".format(
        snapshot=summary.get("snapshot_date"),
        records=summary.get("record_count"),
        public_records=summary.get("auto_publish_count"),
        curator_records=summary.get("curated_review_count"),
        eligible_12m=summary.get("task_a_12m_eligible_count"),
        eligible_36m=summary.get("task_a_36m_eligible_count"),
        leak_status=leak_status,
        invalid_event_count=len(leakage_report.get("records_with_invalid_event_order", [])),
        snapshot_violations=len(leakage_report.get("records_with_snapshot_violations", [])),
        feature_cutoff_violations=len(leakage_report.get("feature_cutoff_violations", [])),
        ingest_lines="\n".join(ingest_lines) if ingest_lines else "- No ingest summary available",
        split_lines="\n".join(split_lines),
        task_a_lines="\n".join(task_a_lines),
        model_name=task_b_baseline.get("model_name"),
        backend=task_b_baseline.get("backend_used"),
        notice_accuracy=task_b_metrics.get("notice_status_accuracy"),
        tag_macro_f1=task_b_metrics.get("tag_macro_f1"),
        coverage=task_b_metrics.get("provenance_coverage"),
    )

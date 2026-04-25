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
    task_a_robustness: dict = None,
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
            task_a_robustness=task_a_robustness or {},
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
            "task_a_robustness": task_a_robustness or {},
            "task_b_baseline": task_b_baseline,
            "ingest_summary": ingest_summary or {},
        },
    )
    return {"markdown": markdown_path, "json": json_path}


def build_results_v0_2_markdown(
    summary: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_a_robustness: dict,
    task_b_baseline: dict,
    run_root: str,
    snapshot_label: str = "2026-03-freeze",
    snapshot_id: str = "public_open_data_2026_03_freeze",
) -> str:
    task_b_metrics = task_b_baseline.get("metrics", {})
    leak_status = "PASS" if leakage_report.get("passed") else "FAIL"
    task_a_12m = _task_a_results_table(task_a_baselines.get("task_a_12m", []))
    task_a_36m = _task_a_results_table(task_a_baselines.get("task_a_36m", []))
    robustness_lines = _robustness_lines(task_a_robustness)
    return """# Real-Data Results (v0.2)

This document captures the **artifacts and metrics produced by the real-data open-data-only release**. Numbers here are derived from the harvested contents of `artifacts/open_data_release/`.

## Source Of Numbers

- Snapshot label: `{snapshot_label}`
- Snapshot ID: `{snapshot_id}`
- Data sources: OpenAlex bulk (open-data), PubMed baseline + updatefiles, Retraction Watch CSV. **Crossref Metadata Plus skipped** in this run (open-data-only profile).
- Run root: `{run_root}`

## Snapshot Summary

| Field | Value |
| --- | --- |
| Snapshot date | `{snapshot_date}` |
| Total records in release | `{record_count}` |
| Public (auto-publish) records | `{auto_publish_count}` |
| Curator-review records | `{curated_review_count}` |
| Task A 12-month eligible | `{task_a_12m_eligible_count}` |
| Task A 36-month eligible | `{task_a_36m_eligible_count}` |
| Noisy-date records excluded | `{task_a_noisy_date_count}` |

Notice-status distribution: {notice_status_counts}.

Subfield distribution: {subfield_counts}.

## Leakage Audit

| Check | Result |
| --- | --- |
| Overall | **{leak_status}** |
| Records checked | `{records_checked}` |
| Leaked banned fields | `{leaked_fields_found}` |
| Records missing feature provenance | `{records_missing_feature_provenance}` |
| Invalid event ordering | `{records_with_invalid_event_order}` |
| Snapshot violations | `{records_with_snapshot_violations}` |
| Feature cutoff violations | `{feature_cutoff_violations}` |

## Task A Baselines

**Task A 12m:**

| Model | AUPRC | AUPRC 95% CI | Precision@1% | Recall@1% | Precision@5% | Recall@5% | ECE |
| --- | --- | --- | --- | --- | --- | --- | --- |
{task_a_12m}

**Task A 36m:**

| Model | AUPRC | AUPRC 95% CI | Precision@1% | Recall@1% | Precision@5% | Recall@5% | ECE |
| --- | --- | --- | --- | --- | --- | --- | --- |
{task_a_36m}

## Task A Robustness (Grouped Holdouts)

{robustness_lines}

Calibration and operating-point visualizations are in `task_a_calibration_curves.svg` and `task_a_pr_curves.svg`.

## Task B Baseline

| Metric | Value |
| --- | --- |
| Notice-status accuracy | `{notice_status_accuracy}` |
| Tag macro-F1 | `{tag_macro_f1}` |
| Provenance coverage | `{provenance_coverage}` |

## Files Delivered By The Run

The harvested release includes the canonical bundle and analysis artifacts: `benchmark_v1.jsonl`, `benchmark_v1.csv`, `summary.json`, `splits.json`, `leakage_report.json`, `task_a_baselines.json`, `task_a_robustness.json`, `task_a_calibration_curves.svg`, `task_a_pr_curves.svg`, `task_b_baseline.json`, `adjudication_queue.csv`, `adjudication_queue_summary.json`, `adjudication_protocol.md`, `internal_curation_queue.json`, `experiment_report.md`, `experiment_report.json`, and the static site.
""".format(
        snapshot_label=snapshot_label,
        snapshot_id=snapshot_id,
        run_root=run_root,
        snapshot_date=summary.get("snapshot_date", "unknown"),
        record_count=summary.get("record_count", "unknown"),
        auto_publish_count=summary.get("auto_publish_count", "unknown"),
        curated_review_count=summary.get("curated_review_count", "unknown"),
        task_a_12m_eligible_count=summary.get("task_a_12m_eligible_count", "unknown"),
        task_a_36m_eligible_count=summary.get("task_a_36m_eligible_count", "unknown"),
        task_a_noisy_date_count=summary.get("task_a_noisy_date_count", "unknown"),
        notice_status_counts=_inline_counts(summary.get("notice_status_counts", {})),
        subfield_counts=_inline_counts(summary.get("subfield_counts", {})),
        leak_status=leak_status,
        records_checked=leakage_report.get("records_checked", 0),
        leaked_fields_found=len(leakage_report.get("leaked_fields_found", [])),
        records_missing_feature_provenance=len(
            leakage_report.get("records_missing_feature_provenance", [])
        ),
        records_with_invalid_event_order=len(
            leakage_report.get("records_with_invalid_event_order", [])
        ),
        records_with_snapshot_violations=len(
            leakage_report.get("records_with_snapshot_violations", [])
        ),
        feature_cutoff_violations=len(leakage_report.get("feature_cutoff_violations", [])),
        task_a_12m=task_a_12m,
        task_a_36m=task_a_36m,
        robustness_lines=robustness_lines,
        notice_status_accuracy=_fmt_metric(task_b_metrics.get("notice_status_accuracy")),
        tag_macro_f1=_fmt_metric(task_b_metrics.get("tag_macro_f1")),
        provenance_coverage=_fmt_metric(task_b_metrics.get("provenance_coverage")),
    )


def update_readme_for_v0_2(
    readme_text: str,
    summary: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_b_baseline: dict,
    results_doc_path: str = "docs/results_v0.2.md",
) -> str:
    status_block = build_readme_v0_2_status(
        summary=summary,
        leakage_report=leakage_report,
        task_a_baselines=task_a_baselines,
        results_doc_path=results_doc_path,
    )
    snapshot_block = build_readme_v0_2_snapshot(
        summary=summary,
        leakage_report=leakage_report,
        task_a_baselines=task_a_baselines,
        task_b_baseline=task_b_baseline,
        results_doc_path=results_doc_path,
    )
    updated = _replace_managed_block(
        readme_text,
        "<!-- LSIB_STATUS_START -->",
        "<!-- LSIB_STATUS_END -->",
        status_block,
    )
    updated = _replace_managed_block(
        updated,
        "<!-- LSIB_RELEASE_SNAPSHOT_START -->",
        "<!-- LSIB_RELEASE_SNAPSHOT_END -->",
        snapshot_block,
    )
    return updated


def build_readme_v0_2_status(
    summary: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    results_doc_path: str = "docs/results_v0.2.md",
) -> str:
    leak_status = "PASS" if leakage_report.get("passed") else "FAIL"
    best_12m = _best_task_a_run(task_a_baselines.get("task_a_12m", []))
    best_36m = _best_task_a_run(task_a_baselines.get("task_a_36m", []))
    return (
        "Status: **v0.2 released.** The real-data open-data-only release now ships "
        "{records} records from OpenAlex + PubMed + Retraction Watch, with a leakage "
        "audit status of **{leak_status}**. Best Task A AUPRC on the primary split: "
        "12m={best_12m}, 36m={best_36m}. Full numbers and interpretation: "
        "[{results_doc_path}]({results_doc_path})."
    ).format(
        records=summary.get("record_count", "unknown"),
        leak_status=leak_status,
        best_12m=_headline_task_a_run(best_12m),
        best_36m=_headline_task_a_run(best_36m),
        results_doc_path=results_doc_path,
    )


def build_readme_v0_2_snapshot(
    summary: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_b_baseline: dict,
    results_doc_path: str = "docs/results_v0.2.md",
) -> str:
    task_b_metrics = task_b_baseline.get("metrics", {})
    best_12m = _best_task_a_run(task_a_baselines.get("task_a_12m", []))
    best_36m = _best_task_a_run(task_a_baselines.get("task_a_36m", []))
    leak_status = "PASS" if leakage_report.get("passed") else "FAIL"
    return """## Release Snapshot

| Metric | Value |
| --- | --- |
| Snapshot date | `{snapshot_date}` |
| Total records | `{record_count}` |
| Public / curator-review records | `{auto_publish_count}` / `{curated_review_count}` |
| Leakage audit | **{leak_status}** |
| Best Task A 12m AUPRC | `{best_12m}` |
| Best Task A 36m AUPRC | `{best_36m}` |
| Task B notice accuracy | `{notice_status_accuracy}` |

Full results: [{results_doc_path}]({results_doc_path}).
""".format(
        snapshot_date=summary.get("snapshot_date", "unknown"),
        record_count=summary.get("record_count", "unknown"),
        auto_publish_count=summary.get("auto_publish_count", "unknown"),
        curated_review_count=summary.get("curated_review_count", "unknown"),
        leak_status=leak_status,
        best_12m=_headline_task_a_run(best_12m),
        best_36m=_headline_task_a_run(best_36m),
        notice_status_accuracy=_fmt_metric(task_b_metrics.get("notice_status_accuracy")),
        results_doc_path=results_doc_path,
    )


def _markdown_report(
    summary: dict,
    splits: dict,
    leakage_report: dict,
    task_a_baselines: dict,
    task_b_baseline: dict,
    ingest_summary: dict,
    task_a_robustness: dict,
) -> str:
    task_a_lines = []
    for task_name, runs in sorted(task_a_baselines.items()):
        task_a_lines.append("### %s" % task_name)
        for run in runs:
            metrics = run["metrics"]
            task_a_lines.append(
                "- `%s` (`%s`): AUPRC=%s (95%% CI %s-%s), Precision@1pct=%s, Precision@5pct=%s, Recall@1pct=%s, Recall@5pct=%s, ECE=%s"
                % (
                    run["model_name"],
                    run["backend_used"],
                    metrics.get("AUPRC"),
                    metrics.get("AUPRC_ci_lower"),
                    metrics.get("AUPRC_ci_upper"),
                    metrics.get("Precision@1pct"),
                    metrics.get("Precision@5pct"),
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
    robustness_lines = _robustness_lines(task_a_robustness)
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

## Task A Robustness (Grouped Holdouts)

Per-model AUPRC across primary time split and grouped-holdout splits
(author cluster, venue, publisher when a statistically usable holdout is
available). Holdouts evaluate whether the baseline is learning a
transferable signal rather than per-group artifacts. A `-` cell means that
the candidate holdout was skipped because it was too small or lacked both
Task A label classes.

{robustness_lines}

## Task B Baseline

- `{model_name}` (`{backend}`): notice accuracy={notice_accuracy}, tag macro-F1={tag_macro_f1}, provenance coverage={coverage}

## Notes

- This report summarizes benchmark artifacts from a single frozen release bundle.
- Group holdout manifests are included only when the held-out group is large enough and, for Task A, contains both label classes.
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
        robustness_lines=robustness_lines,
        model_name=task_b_baseline.get("model_name"),
        backend=task_b_baseline.get("backend_used"),
        notice_accuracy=task_b_metrics.get("notice_status_accuracy"),
        tag_macro_f1=task_b_metrics.get("tag_macro_f1"),
        coverage=task_b_metrics.get("provenance_coverage"),
    )


def build_calibration_svg(task_a_baselines: dict) -> str:
    """Generate a reliability-diagram SVG for all Task A baseline runs.

    Produces a 2-row × 3-column panel grid: rows are horizons (12m, 36m),
    columns are the three baseline models.  Each panel shows:
    - A dashed diagonal (perfect calibration reference)
    - A polyline + dots for the actual mean-predicted-probability vs.
      fraction-positive in each bin
    - A filled background with a thin border

    Returns the SVG markup as a string.  No external dependencies are needed —
    only the ``calibration_curve`` list that ``_ranking_metrics()`` already
    attaches to every ``BaselineRun.metrics`` dict.
    """
    MODELS = [
        "metadata_logistic_baseline",
        "abstract_encoder_baseline",
        "metadata_text_fusion_baseline",
    ]
    MODEL_LABELS = ["metadata", "abstract", "fusion"]
    HORIZONS = ["task_a_12m", "task_a_36m"]
    HORIZON_LABELS = ["12m", "36m"]
    COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    # Layout constants (all in SVG user units = pixels)
    PW, PH = 110, 100   # data-area width / height per panel
    CW, CH = 148, 130   # cell width / height (data area + internal padding)
    PAD_L, PAD_T = 22, 22  # padding from cell top-left to data area top-left
    ML, MT = 52, 44    # outer left / top margin

    NCOLS, NROWS = len(MODELS), len(HORIZONS)
    SVG_W = ML + NCOLS * CW + 10
    SVG_H = MT + NROWS * CH + 30

    def to_svg(px: float, py: float, col: int, row: int):
        """Map (px, py) in [0,1]×[0,1] to SVG pixel coordinates."""
        ox = ML + col * CW + PAD_L
        oy = MT + row * CH + PAD_T
        return ox + px * PW, oy + (1.0 - py) * PH

    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d">' % (SVG_W, SVG_H),
        '  <rect width="%d" height="%d" fill="white"/>' % (SVG_W, SVG_H),
    ]

    # Column headers
    for col, label in enumerate(MODEL_LABELS):
        lx = ML + col * CW + PAD_L + PW // 2
        ly = MT - 8
        out.append(
            '  <text x="%d" y="%d" text-anchor="middle" '
            'font-size="11" font-family="sans-serif">%s</text>' % (lx, ly, label)
        )

    # Row labels
    for row, label in enumerate(HORIZON_LABELS):
        lx = ML - 6
        ly = MT + row * CH + PAD_T + PH // 2
        out.append(
            '  <text x="%d" y="%d" text-anchor="end" dominant-baseline="middle" '
            'font-size="11" font-family="sans-serif">%s</text>' % (lx, ly, label)
        )

    for row, horizon in enumerate(HORIZONS):
        runs = task_a_baselines.get(horizon, [])
        for col, (model_name, color) in enumerate(zip(MODELS, COLORS)):
            bx = ML + col * CW + PAD_L
            by = MT + row * CH + PAD_T
            # Panel background + border
            out.append(
                '  <rect x="%d" y="%d" width="%d" height="%d" '
                'fill="#f7f7f7" stroke="#cccccc" stroke-width="1"/>'
                % (bx, by, PW, PH)
            )
            # Perfect-calibration diagonal (dashed)
            x0, y0 = to_svg(0.0, 0.0, col, row)
            x1, y1 = to_svg(1.0, 1.0, col, row)
            out.append(
                '  <line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" '
                'stroke="#aaaaaa" stroke-width="1" stroke-dasharray="4,3"/>'
                % (x0, y0, x1, y1)
            )
            # Find matching run
            run = next((r for r in runs if r.get("model_name") == model_name), None)
            if run is None:
                continue
            bins = (run.get("metrics") or {}).get("calibration_curve") or []
            if len(bins) < 2:
                continue
            pts = [
                to_svg(b["mean_predicted"], b["fraction_positive"], col, row)
                for b in bins
            ]
            polyline = " ".join("%.1f,%.1f" % (sx, sy) for sx, sy in pts)
            out.append(
                '  <polyline points="%s" fill="none" stroke="%s" stroke-width="2"/>'
                % (polyline, color)
            )
            for sx, sy in pts:
                out.append(
                    '  <circle cx="%.1f" cy="%.1f" r="3" fill="%s"/>' % (sx, sy, color)
                )

    # Axis labels
    ax_x = ML + (NCOLS * CW) // 2
    ax_y = MT + NROWS * CH + 20
    out.append(
        '  <text x="%d" y="%d" text-anchor="middle" '
        'font-size="11" font-family="sans-serif">mean predicted probability</text>'
        % (ax_x, ax_y)
    )
    rot_x = -(MT + NROWS * CH // 2)
    out.append(
        '  <text transform="rotate(-90)" x="%d" y="13" text-anchor="middle" '
        'font-size="11" font-family="sans-serif">fraction positive</text>' % rot_x
    )

    out.append("</svg>")
    return "\n".join(out)


def _replace_managed_block(
    document_text: str, start_marker: str, end_marker: str, replacement: str
) -> str:
    start = document_text.find(start_marker)
    end = document_text.find(end_marker)
    if start == -1 or end == -1 or end < start:
        raise ValueError("managed block markers not found: %s / %s" % (start_marker, end_marker))
    start += len(start_marker)
    return document_text[:start] + "\n" + replacement + "\n" + document_text[end:]


def _best_task_a_run(runs: list):
    if not runs:
        return None
    return max(
        runs,
        key=lambda run: _metric_sort_value((run.get("metrics") or {}).get("AUPRC")),
    )


def _metric_sort_value(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return float("-inf")


def _headline_task_a_run(run: dict) -> str:
    if not run:
        return "n/a"
    metrics = run.get("metrics", {})
    return "%s (%s, 95%% CI %s-%s)" % (
        _pretty_model_name(run.get("model_name")),
        _fmt_metric(metrics.get("AUPRC")),
        _fmt_metric(metrics.get("AUPRC_ci_lower")),
        _fmt_metric(metrics.get("AUPRC_ci_upper")),
    )


def _task_a_results_table(runs: list) -> str:
    if not runs:
        return "| _no runs_ | - | - | - | - | - | - | - |"
    return "\n".join(_task_a_results_row(run) for run in runs)


def _task_a_results_row(run: dict) -> str:
    metrics = run.get("metrics", {})
    return "| {model} | {auprc} | {ci} | {p1} | {r1} | {p5} | {r5} | {ece} |".format(
        model=_pretty_model_name(run.get("model_name")),
        auprc=_fmt_metric(metrics.get("AUPRC")),
        ci="%s-%s"
        % (
            _fmt_metric(metrics.get("AUPRC_ci_lower")),
            _fmt_metric(metrics.get("AUPRC_ci_upper")),
        ),
        p1=_fmt_metric(metrics.get("Precision@1pct")),
        r1=_fmt_metric(metrics.get("Recall@1pct")),
        p5=_fmt_metric(metrics.get("Precision@5pct")),
        r5=_fmt_metric(metrics.get("Recall@5pct")),
        ece=_fmt_metric(metrics.get("ECE")),
    )


def _pretty_model_name(model_name: str) -> str:
    labels = {
        "metadata_logistic_baseline": "metadata_logistic",
        "abstract_encoder_baseline": "abstract_encoder (hashing)",
        "metadata_text_fusion_baseline": "metadata + text fusion",
    }
    return labels.get(model_name, model_name or "unknown")


def _fmt_metric(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return "%.3f" % value
    return str(value)


def _inline_counts(counts: dict) -> str:
    if not counts:
        return "none"
    return ", ".join("%s=%s" % (key, counts[key]) for key in sorted(counts))


def build_pr_curve_svg(task_a_baselines: dict) -> str:
    """Generate an SVG of sparse PR curves from stored operating points.

    Each panel uses the threshold scan emitted by ``_ranking_metrics()`` to
    plot recall on the x-axis and precision on the y-axis at the top-0.5%,
    1%, 2%, 5%, and 10% operating points.
    """
    MODELS = [
        "metadata_logistic_baseline",
        "abstract_encoder_baseline",
        "metadata_text_fusion_baseline",
    ]
    MODEL_LABELS = ["metadata", "abstract", "fusion"]
    HORIZONS = ["task_a_12m", "task_a_36m"]
    HORIZON_LABELS = ["12m", "36m"]
    COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c"]
    THRESHOLD_KEYS = [
        "top_0.5pct",
        "top_1pct",
        "top_2pct",
        "top_5pct",
        "top_10pct",
    ]

    PW, PH = 110, 100
    CW, CH = 148, 130
    PAD_L, PAD_T = 22, 22
    ML, MT = 52, 44

    NCOLS, NROWS = len(MODELS), len(HORIZONS)
    SVG_W = ML + NCOLS * CW + 10
    SVG_H = MT + NROWS * CH + 30

    def to_svg(recall: float, precision: float, col: int, row: int):
        ox = ML + col * CW + PAD_L
        oy = MT + row * CH + PAD_T
        return ox + recall * PW, oy + (1.0 - precision) * PH

    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d">' % (SVG_W, SVG_H),
        '  <rect width="%d" height="%d" fill="white"/>' % (SVG_W, SVG_H),
    ]

    for col, label in enumerate(MODEL_LABELS):
        lx = ML + col * CW + PAD_L + PW // 2
        ly = MT - 8
        out.append(
            '  <text x="%d" y="%d" text-anchor="middle" '
            'font-size="11" font-family="sans-serif">%s</text>' % (lx, ly, label)
        )

    for row, label in enumerate(HORIZON_LABELS):
        lx = ML - 6
        ly = MT + row * CH + PAD_T + PH // 2
        out.append(
            '  <text x="%d" y="%d" text-anchor="end" dominant-baseline="middle" '
            'font-size="11" font-family="sans-serif">%s</text>' % (lx, ly, label)
        )

    for row, horizon in enumerate(HORIZONS):
        runs = task_a_baselines.get(horizon, [])
        for col, (model_name, color) in enumerate(zip(MODELS, COLORS)):
            bx = ML + col * CW + PAD_L
            by = MT + row * CH + PAD_T
            out.append(
                '  <rect x="%d" y="%d" width="%d" height="%d" '
                'fill="#f7f7f7" stroke="#cccccc" stroke-width="1"/>'
                % (bx, by, PW, PH)
            )
            run = next((r for r in runs if r.get("model_name") == model_name), None)
            if run is None:
                continue
            operating_points = (
                (run.get("metrics") or {}).get("precision_recall_at_thresholds") or {}
            )
            points = []
            for key in THRESHOLD_KEYS:
                point = operating_points.get(key)
                if not point:
                    continue
                points.append(to_svg(point["recall"], point["precision"], col, row))
            if len(points) >= 2:
                out.append(
                    '  <polyline points="%s" fill="none" stroke="%s" stroke-width="2"/>'
                    % (" ".join("%.1f,%.1f" % (sx, sy) for sx, sy in points), color)
                )
            for sx, sy in points:
                out.append(
                    '  <circle cx="%.1f" cy="%.1f" r="3" fill="%s"/>' % (sx, sy, color)
                )

    ax_x = ML + (NCOLS * CW) // 2
    ax_y = MT + NROWS * CH + 20
    out.append(
        '  <text x="%d" y="%d" text-anchor="middle" '
        'font-size="11" font-family="sans-serif">recall</text>' % (ax_x, ax_y)
    )
    rot_x = -(MT + NROWS * CH // 2)
    out.append(
        '  <text transform="rotate(-90)" x="%d" y="13" text-anchor="middle" '
        'font-size="11" font-family="sans-serif">precision</text>' % rot_x
    )

    out.append("</svg>")
    return "\n".join(out)


def _robustness_lines(task_a_robustness: dict) -> str:
    if not task_a_robustness:
        return "- No robustness results available."

    horizons = ["12m", "36m"]
    holdout_suffixes = [
        ("primary", ""),
        ("author_cluster_holdout", "_author_cluster_holdout"),
        ("venue_holdout", "_venue_holdout"),
        ("publisher_holdout", "_publisher_holdout"),
    ]
    sections = []
    for horizon in horizons:
        header = "### Task A %s — AUPRC by model x split" % horizon
        header_row = "| model | " + " | ".join(label for label, _ in holdout_suffixes) + " |"
        divider = "| --- | " + " | ".join("---" for _ in holdout_suffixes) + " |"
        rows = [header, "", header_row, divider]
        model_names = set()
        for _, suffix in holdout_suffixes:
            manifest_name = "task_a_%s%s" % (horizon, suffix)
            for run in task_a_robustness.get(manifest_name, []) or []:
                model_names.add(run["model_name"])
        for model_name in sorted(model_names):
            cells = ["`%s`" % _pretty_model_name(model_name)]
            for _, suffix in holdout_suffixes:
                manifest_name = "task_a_%s%s" % (horizon, suffix)
                runs = task_a_robustness.get(manifest_name, []) or []
                match = next((r for r in runs if r["model_name"] == model_name), None)
                cell = "-"
                if match is not None:
                    auprc = (match.get("metrics") or {}).get("AUPRC")
                    if auprc is not None:
                        cell = "%.3g" % auprc
                cells.append(cell)
            rows.append("| " + " | ".join(cells) + " |")
        sections.append("\n".join(rows))
    return "\n\n".join(sections)

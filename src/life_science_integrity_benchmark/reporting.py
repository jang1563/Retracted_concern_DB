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
(author cluster, venue, publisher). Holdouts evaluate whether the
baseline is learning a transferable signal rather than per-venue or
per-publisher artifacts.

{robustness_lines}

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
            cells = ["`%s`" % model_name]
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

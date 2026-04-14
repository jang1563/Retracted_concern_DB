# Real-Data Results (v0.2 draft)

> **Status: placeholder.** This document is a skeleton prepared while the real-data pipeline is still running on Cayuga (job `2789241`, submitted 2026-04-13). Numbers here will be replaced with the output of `scripts/cayuga/harvest_open_data_release_to_local.sh` when the job finishes. Until then, the authoritative demo numbers are in [results.md](results.md).

## Source Of Numbers

- Snapshot label: `2026-03-freeze`
- Snapshot ID: `public_open_data_2026_03_freeze`
- Data sources: OpenAlex bulk (open-data), PubMed baseline + updatefiles, Retraction Watch CSV. **Crossref Metadata Plus skipped** in this run (open-data-only profile).
- Raw snapshot: `openalex=2127 ingest files`, `pubmed=1412 ingest files`, `official_notices=1 ingest file` (plus sidecars); total registered files: `3540`.
- Run root: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1`.

## How To Reproduce

On Cayuga, once the raw snapshot at `$RUN_ROOT/raw/public_open_data_snapshot/` is present and non-empty for `openalex/`, `official_notices/`, and `pubmed/`:

```bash
./scripts/cayuga/submit_open_data_downstream_only.sh "$RUN_ROOT"
```

This runs `register → ingest → materialize → validate → build-core → build-splits → audit-leakage → train-task-a → train-task-b → build-site → build-report` against the staged raw snapshot, with `--time=5-00:00:00`.

After completion, harvest the release to your local repo:

```bash
./scripts/cayuga/harvest_open_data_release_to_local.sh cayuga-phobos "$RUN_ROOT"
```

## Snapshot Summary

_TODO: replace with output of harvest script._

| Field | Value |
| --- | --- |
| Snapshot date | _TBD_ |
| Total records in release | _TBD_ |
| Public (auto-publish) records | _TBD_ |
| Curator-review records | _TBD_ |
| Task A 12-month eligible | _TBD_ |
| Task A 36-month eligible | _TBD_ |
| Noisy-date records excluded | _TBD_ |

Notice-status distribution: _TBD_.

Subfield distribution: _TBD_.

## Leakage Audit

_TODO: confirm **PASS** status and paste counts from `leakage_report.json`._

| Check | Result |
| --- | --- |
| Overall | _TBD_ |
| Records checked | _TBD_ |
| Leaked banned fields | _TBD_ |
| Feature cutoff violations | _TBD_ |
| Snapshot violations | _TBD_ |

## Task A Baselines

_TODO: replace with real metrics. Three models per horizon as in the synthetic demo._

**Task A 12m:**

| Model | AUPRC | Recall@1% | Recall@5% | ECE |
| --- | --- | --- | --- | --- |
| metadata_logistic | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| abstract_encoder (hashing) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| metadata + text fusion | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

**Task A 36m:**

| Model | AUPRC | Recall@1% | Recall@5% | ECE |
| --- | --- | --- | --- | --- |
| metadata_logistic | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| abstract_encoder (hashing) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| metadata + text fusion | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

Subfield-sliced AUPRC and robustness-split numbers: _TBD — pull from `task_a_baselines.json` and `task_a_robustness.json` (the latter covers all 8 manifests: primary + author_cluster / venue / publisher holdouts × 2 horizons)._

## Task B Baseline

_TODO: confirm the keyword-rules-over-provenance baseline still produces calibrated outputs on real data._

| Metric | Value |
| --- | --- |
| Notice-status accuracy | _TBD_ |
| Tag macro-F1 | _TBD_ |
| Provenance coverage | _TBD_ |

## Interpretation Notes (fill in after harvest)

Things worth commenting on when the real numbers land:

- Scale: how does record count compare to 16 synthetic? (Expected: 10⁶ OpenAlex works filtered to life-science + PubMed-indexed.)
- Base rate of `retracted` vs `editorial_notice` vs `none_known_at_snapshot` on real data.
- Whether the metadata baseline actually beats the hashing-text baseline, or vice versa, at real scale.
- Subfield-AUPRC — is any subfield still degenerate at 0.0? If so, at what cohort size?
- Horizon effect: does 36m AUPRC still dominate 12m, or does the picture change at scale?
- Leakage audit — the real test of whether the feature-cutoff discipline holds up on messy real data.

## Comparison To Synthetic Demo

See [results.md](results.md) for the synthetic numbers. The demo was a shape-of-output reference; this document will be the actual benchmark result.

## Files Delivered By The Run

On successful completion, `artifacts/open_data_release/` and `artifacts/open_data_site/` will contain the same file set as the synthetic demo (`benchmark_v1.jsonl`, `summary.json`, `splits.json`, `leakage_report.json`, `task_a_baselines.json`, `task_b_baseline.json`, `adjudication_queue.csv`, `experiment_report.md`, site HTML pages), derived from real data instead of the bundled sample.

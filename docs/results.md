# Demo Results

This document captures the **artifacts and metrics produced by a clean `demo` run** on the bundled synthetic sample corpus. It is intended for readers who want to see what the benchmark produces without cloning and running it themselves.

> These numbers are a **protocol demonstration** on 16 synthetic records. They demonstrate that every stage of the pipeline runs end-to-end and produces the expected artifact shape. They are **not** real-data benchmark results. Real-data numbers will land in a future release.

## How To Reproduce

```bash
PYTHONPATH=src python3 -m life_science_integrity_benchmark.cli demo
```

No dependencies are required. Outputs appear under `artifacts/sample_release/` and `artifacts/site/`.

## Snapshot Summary

| Field | Value |
| --- | --- |
| Snapshot date | `2026-04-09` |
| Total records in release | `16` |
| Public (auto-publish) records | `6` |
| Curator-review records | `5` |
| Task A 12-month eligible | `16` |
| Task A 36-month eligible | `12` |
| Noisy-date records (excluded from primary Task A) | `0` |
| Total extension tags attached | `10` |

Notice-status distribution:

| Status | Count |
| --- | --- |
| `none_known_at_snapshot` | 10 |
| `editorial_notice` | 3 |
| `retracted` | 3 |

Subfield distribution: `bioinformatics=5`, `biology=5`, `biomedicine=6`.

## Leakage Audit

The `audit-leakage` CLI step produced a clean report on this release:

| Check | Result |
| --- | --- |
| Overall | **PASS** |
| Records checked | 16 |
| Leaked banned fields | 0 |
| Records missing feature provenance | 0 |
| Invalid event ordering | 0 |
| Snapshot violations | 0 |
| Feature cutoff violations | 0 |

Banned post-publication fields screened out of Task A features include `notice_status`, `core_tags`, `extension_tags`, `first_signal_date`, `first_notice_date`, `source_names`, `source_urls`, `allowed_feature_view`, `public_summary`, `auto_publish`, and `curator_review_required`.

Task A publication-time feature set: `title`, `abstract`, `venue`, `publisher`, `publication_year`, `subfield`, `is_pubmed_indexed`, `openalex_life_science_score`, `references_count`, `author_history_signal_count`, `journal_history_signal_count`, `oa_status`, `authors`, `institutions`.

## Splits

14 split manifests are produced per release: a primary time split and three grouped holdouts (author cluster, venue, publisher) for each of Task A 12m, Task A 36m, and Task B, plus noisy-date analysis splits.

| Split | Kind | Train | Val | Test |
| --- | --- | --- | --- | --- |
| `task_a_12m` | time | 8 | 3 | 5 |
| `task_a_12m_author_cluster_holdout` | group | 8 | 3 | 1 |
| `task_a_12m_venue_holdout` | group | 7 | 2 | 3 |
| `task_a_12m_publisher_holdout` | group | 6 | 2 | 5 |
| `task_a_36m` | time | 6 | 2 | 4 |
| `task_a_36m_author_cluster_holdout` | group | 6 | 2 | 1 |
| `task_a_36m_venue_holdout` | group | 6 | 2 | 2 |
| `task_a_36m_publisher_holdout` | group | 4 | 1 | 5 |
| `task_b` | time | 8 | 3 | 5 |
| `task_b_author_cluster_holdout` | group | 8 | 3 | 1 |
| `task_b_venue_holdout` | group | 7 | 2 | 3 |
| `task_b_publisher_holdout` | group | 6 | 2 | 5 |

## Task A Baselines

Three baseline models are included: a metadata-only logistic regression, an abstract-text hashing encoder, and a metadata+text fusion model.

**Task A 12m** (horizon = 12 months after publication):

| Model | AUPRC | Recall@1% | Recall@5% | ECE |
| --- | --- | --- | --- | --- |
| metadata_logistic | 0.700 | 0.500 | 0.500 | 0.328 |
| abstract_encoder (hashing) | 0.700 | 0.500 | 0.500 | 0.478 |
| metadata + text fusion | 0.700 | 0.500 | 0.500 | 0.323 |

**Task A 36m** (horizon = 36 months):

| Model | AUPRC | Recall@1% | Recall@5% | ECE |
| --- | --- | --- | --- | --- |
| metadata_logistic | 0.917 | 0.333 | 0.333 | 0.093 |
| abstract_encoder (hashing) | 0.806 | 0.333 | 0.333 | 0.116 |
| metadata + text fusion | 0.917 | 0.333 | 0.333 | 0.291 |

On the sample corpus, AUPRC-by-subfield reveals a degenerate `biology=0.0` in every Task A configuration. That is an artifact of a small synthetic dataset where the test fold's `biology` slice has no positive label under this split seed; it is a useful reminder that subfield-sliced metrics are sensitive to cohort size and that real-data evaluation needs larger cohorts.

## Task A Robustness (Grouped Holdouts)

New in this release: every Task A baseline now also runs on the three grouped-holdout manifests (author cluster, venue, publisher) in addition to the primary time split. This is the empirical test of whether the model is learning a transferable signal or a per-venue / per-publisher artifact.

**Task A 12m — AUPRC by model × split:**

| Model | primary | author_cluster_holdout | venue_holdout | publisher_holdout |
| --- | --- | --- | --- | --- |
| metadata_logistic | 0.700 | **0.000** | 1.000 | 1.000 |
| abstract_encoder (hashing) | 0.700 | **0.000** | 0.500 | 1.000 |
| metadata + text fusion | 0.700 | **0.000** | 1.000 | 1.000 |

**Task A 36m — AUPRC by model × split:**

| Model | primary | author_cluster_holdout | venue_holdout | publisher_holdout |
| --- | --- | --- | --- | --- |
| metadata_logistic | 0.917 | 1.000 | 1.000 | 1.000 |
| abstract_encoder (hashing) | 0.806 | 1.000 | 1.000 | 1.000 |
| metadata + text fusion | 0.917 | 1.000 | 1.000 | 1.000 |

The 12m author-cluster holdout collapsing to AUPRC=0.000 across all three baselines is an artifact of the 1-record test fold (the held-out cluster has a single positive in the test slice that the models consistently rank low). On 16 synthetic records these numbers are not individually informative, but they demonstrate that the robustness harness does surface distributional shift — on real data the same table will show meaningful deltas between the primary split and the grouped holdouts, quantifying how much of each baseline's headline AUPRC comes from venue / publisher / authorship recurrence versus generalizable signal.

The raw per-split runs, including all metrics (AUPRC, Recall@1%, Recall@5%, ECE, subfield-AUPRC), are in [artifacts/sample_release/task_a_robustness.json](../artifacts/sample_release/task_a_robustness.json) after running the demo.

## Task B Baseline

A keyword-rules-over-provenance baseline classifies notice status and applies issue tags from snapshot-visible evidence:

| Metric | Value |
| --- | --- |
| Notice-status accuracy | 1.000 |
| Tag macro-F1 | 0.982 |
| Provenance coverage | 0.688 |

On synthetic data these are near-ceiling; the informative measurement is that the pipeline produces calibrated, provenance-backed outputs at all.

## Site Output

The `build-site` step emits a static, read-only evidence browser under `artifacts/site/`:

```
artifacts/site/
├── index.html          # paginated list of public records, with DOI/title/tag search
├── policy.html         # governance and public-display rules
├── changes.html        # per-release change log
├── records/            # one HTML page per public record
├── records.json        # structured record index
└── styles.css
```

The landing page displays the snapshot date, public-record count, and the explicit disclaimer *"This page summarizes public integrity signals and notices. It is not a determination of misconduct."* Records with non-notice external signals are not rendered publicly; they appear in `internal_curation_queue.json` for curator review instead.

## Release Files

```
artifacts/sample_release/
├── benchmark_v1.jsonl               # canonical per-record release
├── benchmark_v1.csv                 # same content, CSV
├── summary.json                     # counts and snapshot metadata
├── splits.json                      # 14 split manifests
├── leakage_report.json              # audit-leakage output
├── task_a_baselines.json            # primary-split baselines (2 horizons × 3 models)
├── task_a_robustness.json           # baselines across 8 manifests (primary + 3 holdouts × 2 horizons)
├── task_b_baseline.json             # Task B keyword baseline
├── adjudication_queue.csv           # double-review queue for labeled records
├── adjudication_queue_summary.json
├── adjudication_protocol.md         # review policy frozen into this release
├── internal_curation_queue.json     # non-notice signals held from public display
├── experiment_report.md             # this report in narrative form
└── experiment_report.json           # machine-readable twin of the report
```

## Test Suite

The repository ships with `tests/test_benchmark.py` covering dataset logic, label derivation, split construction, leakage auditing, baseline model fitting, site generation, ingest manifests, and the vendor-archive → raw-snapshot pipeline. All 20 tests pass on Python 3.13 with zero external dependencies.

## What Real-Data Results Will Add

When the in-progress OpenAlex + Retraction Watch + PubMed ingest completes, a future release will replace this document's numbers with:

- Record counts at the 10⁶ scale (vs. 16 here)
- Real distribution of `notice_status`, not a synthetic balance
- Subfield-AUPRC slices at sizes where the metrics are informative
- Comparison of metadata-only vs. text baselines on real abstracts
- Task B evaluation against real adjudicated labels

Until then, treat this document as a *shape-of-output* reference.

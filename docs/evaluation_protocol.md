# Evaluation Protocol

## Task A

- Objective: rank papers by likelihood of receiving a public integrity signal or official notice within a fixed horizon
- Inputs: publication-time metadata, abstract, venue, authorship metadata, and time-censored history features
- Horizons:
  - `12m`
  - `36m`

## Task B

- Objective: aggregate snapshot-visible evidence into notice status and issue tags
- Inputs: provenance entries and permitted evidence summaries visible by snapshot date

## Splits

- Primary split: time-based train / validation / test
- Noisy-date analysis split:
  - `task_a_12m_noisy_date`
  - `task_a_36m_noisy_date`
- Additional robustness splits:
  - author cluster holdout
  - venue holdout
  - publisher holdout
- Grouped holdouts are emitted only when the held-out group is large enough to interpret; Task A holdouts also require both positive and negative labels in the held-out group.

## Metrics

- `Task A`
  - `AUPRC`
  - `Recall@1pct`
  - `Recall@5pct`
  - `ECE`
  - subfield-sliced `AUPRC`
- `Task B`
  - notice status accuracy
  - tag macro-F1
  - provenance coverage

## Audit Requirements

- No Task A feature may depend on post-publication evidence
- Feature cutoff dates must be present for time-censored history features
- Snapshot-visible evidence must not postdate the declared snapshot
- `year_imputed` publication dates must never appear in the primary Task A benchmark

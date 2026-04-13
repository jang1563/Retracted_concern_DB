# Data Card

## Purpose

This benchmark supports research on triaging life-science papers for further integrity review. It is not intended to determine misconduct or fraud.

## Unit Of Analysis

- One paper per row
- Public key: DOI
- Internal key: stable synthetic ID for joins

## Scope

- Included: peer-reviewed `article` and `review`
- Years: `2000-2024`
- Domain: life sciences defined by `PubMed indexing OR OpenAlex life-science score`
- Excluded: editorials, protocols, notices, corrections, preprints, and other non-target publication types

## Labels

- `notice_status`
  - `none_known_at_snapshot`
  - `editorial_notice`
  - `retracted`
- `core_tags`
  - `retraction`
  - `expression_of_concern`
  - `major_correction`
- `extension_tags`
  - `community_flag`
  - `image_issue`
  - `paper_mill_signal`
  - `tortured_phrase`
  - `metadata_anomaly`

## Task Definitions

- `Task A`
  - Input: publication-time features only
  - Target: `any_signal_or_notice_within_12m` or `within_36m`
- `Task B`
  - Input: snapshot-visible evidence and provenance
  - Target: `notice_status` plus issue tags

## Leakage Controls

- Publication-time cutoff stored as `task_a_feature_cutoff_date`
- History features tracked with explicit `author_history_cutoff_date` and `journal_history_cutoff_date`
- Post-publication signals and notices are excluded from Task A features
- Split artifacts include time splits and explicit grouped holdout variants

## Known Limitations

- The repository currently ships with synthetic sample data for offline execution
- Real-source ingest and adjudicated subsets are planned next steps
- Extension signals are not automatically eligible for public display

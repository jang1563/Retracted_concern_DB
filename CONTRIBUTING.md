# Contributing

Thank you for considering a contribution. Because this project handles integrity signals about named researchers, contributions are reviewed against a governance bar, not just code quality.

## Before You Open An Issue Or PR

Please read:

- [README.md](README.md) — what this is and is not
- [ETHICS.md](ETHICS.md) — intended use and non-goals
- [docs/governance_policy.md](docs/governance_policy.md) — public display rules
- [docs/source_rights_matrix.md](docs/source_rights_matrix.md) — what can be redistributed
- [docs/evaluation_protocol.md](docs/evaluation_protocol.md) — task and split definitions

## Ways To Contribute

### Report a record-level dispute or correction
Do **not** open a public GitHub issue for this. Use the dispute contact on the public release's site page so the record can be handled through the correction workflow rather than in a public thread.

### Propose a new signal source
Open an issue with:
- Source name, URL, and license
- How records are identified (DOI-level? journal-level? author-level?)
- Redistribution rights at the record level (full text, metadata, or link-only)
- Whether the source is an official notice or a non-notice external signal
- A small sample of records (rights permitting)

New sources that cannot be integrated as rights-safe link-only provenance will not be merged.

### Improve the benchmark core
PRs are welcome for:
- Task A / Task B model baselines
- Leakage audits, additional split strategies, metric slices
- Evidence browser accessibility, policy page clarity
- Documentation, tests, reproducibility
- Operational hardening of the ingest pipeline

### Report a vulnerability or rights leak
Follow [SECURITY.md](SECURITY.md). Do not disclose publicly until coordinated.

## Pull Request Standards

- Keep changes scoped. Split unrelated changes across PRs.
- Do not weaken guardrails in the governance policy or rights matrix without explicit discussion in an issue first.
- Do not add features that would cause the public site to display numeric risk scores, per-author aggregates, or non-notice external signals without curator review.
- Add tests for new dataset logic, auditing, modeling, or site generation behavior (`tests/` already covers these classes).
- Update `CHANGELOG` entries or relevant `docs/` files when behavior changes.
- Negative state in any label or feature stays `none_known_at_snapshot`. Never "clean."

## Commit And Review Etiquette

- Prefer small, reviewable commits with factual messages.
- Do not amend after review has started.
- Expect two classes of review: code correctness and governance impact. Governance review may take longer.

## Code Of Conduct

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

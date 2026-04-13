# Ethics And Intended Use

## One-Line Summary

This benchmark supports *triage* of life-science papers for human integrity review. It is not a misconduct detector and does not determine guilt.

## Intended Uses

- Developing and comparing ML models that rank papers for further integrity review
- Studying patterns in publicly available retraction, correction, and expression-of-concern records
- Evaluating provenance-aware evidence aggregation
- Research on leakage, temporal drift, and horizon-based label shift in integrity signals
- Teaching and replication of the evaluation protocol

## Uses This Project Does Not Support

The project does not support, and public-facing work built on it must not be framed as, any of the following:

- Determination of research misconduct, fraud, or fabrication
- Public "risk scores," accusations, or confidence percentages about specific papers, authors, institutions, journals, or publishers
- Leaderboards ranking authors, institutions, venues, or publishers by predicted integrity risk
- Employment, funding, tenure, promotion, editorial, or peer-review decisions about named individuals
- Mass outreach to authors based on model outputs without human review and without a correction/dispute channel
- Any decision made automatically from model output alone; every downstream decision must include a named human reviewer

## Guardrails Built Into The Release

- Negative state is `none_known_at_snapshot`, never "clean." Absence of a signal at a snapshot date does not imply integrity.
- Public site pages never display numeric risk scores.
- Official notices may auto-publish with source-linked factual summaries; non-notice external signals require curator review before public display.
- Extension signals are link-only unless explicit redistribution rights exist.
- Every release carries a snapshot date, change log, and dispute contact.
- `year_imputed` publication dates are excluded from the primary Task A benchmark.

## If You Redistribute Or Extend This Work

- Preserve these guardrails, or document in-band where you deviate and why.
- Do not remove the "not a fraud detector" framing from public surfaces built on this benchmark.
- Do not publish per-author, per-institution, per-journal, or per-publisher aggregate risk displays.
- Respect the source rights matrix in [docs/source_rights_matrix.md](docs/source_rights_matrix.md). Link-only sources must stay link-only.
- If you release model outputs, ship them with a dispute/correction workflow equivalent to the one in this repository.

## Reporting Concerns

- Dispute a specific record: use the dispute contact on the public release's site page.
- Report misuse of this benchmark: open an issue on this repository, or email the maintainer listed in [CITATION.cff](CITATION.cff).
- Report a vulnerability or inadvertent rights leak in a release: follow [SECURITY.md](SECURITY.md) for responsible disclosure.

## Why This Matters

Research-integrity signals are incomplete, latency-prone, and carry real reputational consequences for named individuals. Triage tooling that does not respect rights, provenance, and due process can do concrete harm even when technically correct on aggregate metrics. The scaffolding in this repository — curator gates, auto-publish restrictions, leakage audits, dispute workflow — exists because those are the minimum conditions under which this work can be done responsibly.

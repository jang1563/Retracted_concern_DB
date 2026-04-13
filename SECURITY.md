# Security And Responsible Disclosure

## Scope

This project produces public releases that aggregate bibliographic metadata and official integrity notices. The most load-bearing classes of issue are:

1. Inadvertent redistribution of content that is not rights-safe (full text, images, license-restricted fields)
2. Leakage of identifiers, internal curator notes, or unreviewed external signals into the public release
3. Code execution, path traversal, or injection issues in the ingest or site-generation pipeline
4. Authentication or credential exposure in commit history or runtime state

## What To Report Privately

Report privately, before opening a public issue, for anything in the list above. In particular:

- A public release contains content you believe is not license-permitted to redistribute
- A public release contains a record that should have been curator-gated but was auto-published
- A vulnerability in the ingest pipeline lets a crafted source shard execute code, read outside the snapshot, or corrupt manifests
- Credentials, tokens, or access keys are present in the repository history or a released artifact

## How To Report

Email the maintainer listed in [CITATION.cff](CITATION.cff), or open a GitHub Security Advisory against this repository. Please include:

- Affected version / commit / release tag
- A minimal reproduction or evidence
- The URL of any public artifact involved
- Your preferred attribution (or "anonymous")

## Coordination Timeline

- Acknowledgment: within 5 business days.
- Initial assessment: within 10 business days.
- Fix or rights-takedown for released artifacts: as quickly as the issue class warrants. Rights issues take priority over latency issues.

## Out Of Scope

- Disputes about whether a specific record *should* have received an integrity notice. These go through the record-level dispute workflow on the public site, not through security.
- Generic automated scanner reports with no demonstrated impact.

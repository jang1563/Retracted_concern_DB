# Source Rights Matrix

## Public Benchmark Core

| Source class | Example | Stored in public release | Notes |
| --- | --- | --- | --- |
| Open bibliographic metadata | OpenAlex bulk-style metadata | Yes | Metadata only |
| Official notice metadata | Generic local notice export derived from official notices | Yes | Factual metadata and links only |
| DOI-level identifiers | DOI | Yes | Public key |

## Restricted Or Link-Only Sources

| Source class | Stored in release | Public site behavior | Internal behavior |
| --- | --- | --- | --- |
| Community discussion platforms | No raw text | No auto-publication | Link-only provenance |
| Paper-mill watchlists | No raw PDFs or screenshots | No auto-publication | Link-only provenance |
| Full text / PDFs without redistribution rights | No | Not shown | Internal processing only if rights allow |
| Images without redistribution rights | No | Not shown | Reserved for future multimodal track |

## Operational Rule

- Public benchmark exports only rights-safe metadata and annotations
- Internal pipelines may retain provenance references, quarantine rows, and manifest hashes for auditability
- Public display of non-notice external signals requires curator review

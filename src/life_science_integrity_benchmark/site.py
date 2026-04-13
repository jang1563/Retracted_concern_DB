"""Static evidence-browser generator."""

import html
import json
from pathlib import Path
from typing import List

from .constants import POLICY_CONTACT, SITE_DISCLAIMER
from .types import BenchmarkRecord, SourceProvenance
from .utils import slugify, write_json


def build_site(records: List[BenchmarkRecord], output_dir: Path, summary: dict) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    stale_internal_file = output_dir / "curation_queue.html"
    if stale_internal_file.exists():
        stale_internal_file.unlink()
    public_records = [record for record in records if record.auto_publish]

    record_dir = output_dir / "records"
    record_dir.mkdir(parents=True, exist_ok=True)

    for record in public_records:
        path = record_dir / ("%s.html" % slugify(record.doi))
        path.write_text(_record_page(record), encoding="utf-8")

    (output_dir / "styles.css").write_text(_styles_css(), encoding="utf-8")
    (output_dir / "index.html").write_text(_index_page(public_records, summary), encoding="utf-8")
    (output_dir / "policy.html").write_text(_policy_page(summary), encoding="utf-8")
    (output_dir / "changes.html").write_text(_changes_page(summary), encoding="utf-8")

    records_json = [
        {
            "doi": record.doi,
            "title": record.title,
            "subfield": record.subfield,
            "notice_status": record.notice_status,
            "tags": list(record.core_tags),
            "page": "records/%s.html" % slugify(record.doi),
        }
        for record in public_records
    ]
    write_json(output_dir / "records.json", records_json)
    return {
        "index": output_dir / "index.html",
        "policy": output_dir / "policy.html",
        "changes": output_dir / "changes.html",
    }


def export_internal_curation_queue(records: List[BenchmarkRecord], output_path: Path) -> Path:
    queue = [
        {
            "doi": record.doi,
            "title": record.title,
            "publication_date": record.publication_date,
            "subfield": record.subfield,
            "notice_status": record.notice_status,
            "extension_tags": list(record.extension_tags),
            "public_summary": record.public_summary,
            "provenance": [
                {
                    "source_name": entry.source_name,
                    "source_url": entry.source_url,
                    "event_date": entry.event_date,
                    "event_kind": entry.event_kind,
                    "observed_label": entry.observed_label,
                    "summary": entry.summary,
                }
                for entry in record.provenance
            ],
        }
        for record in records
        if record.curator_review_required
    ]
    write_json(output_path, queue)
    return output_path


def _index_page(records: List[BenchmarkRecord], summary: dict) -> str:
    cards = "\n".join(_record_card(record) for record in records)
    search_blob = html.escape(
        json.dumps(
            [
                {
                    "doi": record.doi,
                    "title": record.title,
                    "subfield": record.subfield,
                    "notice_status": record.notice_status,
                    "tags": list(record.core_tags),
                    "href": "records/%s.html" % slugify(record.doi),
                }
                for record in records
            ]
        )
    )
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Life-Science Integrity Signals Browser</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <div class="shell">
    <header class="hero">
      <p class="eyebrow">Read-only evidence browser</p>
      <h1>Life-Science Integrity Signals</h1>
      <p class="lede">%s</p>
      <div class="hero-meta">
        <span>Snapshot %s</span>
        <span>%s public records</span>
        <span><a href="policy.html">Policy</a></span>
        <span><a href="changes.html">Change log</a></span>
      </div>
    </header>
    <section class="search-panel">
      <label for="search">Search DOI, title, or official tag</label>
      <input id="search" type="search" placeholder="glioma, 10.5555..., retraction">
    </section>
    <section id="cards" class="cards">
      %s
    </section>
  </div>
  <script id="records-data" type="application/json">%s</script>
  <script>
    const input = document.getElementById('search');
    const cards = Array.from(document.querySelectorAll('.record-card'));
    input.addEventListener('input', () => {
      const query = input.value.toLowerCase().trim();
      cards.forEach((card) => {
        card.hidden = query && !card.dataset.search.includes(query);
      });
    });
  </script>
</body>
</html>
""" % (
        html.escape(SITE_DISCLAIMER),
        html.escape(summary["snapshot_date"]),
        len(records),
        cards,
        search_blob,
    )


def _record_card(record: BenchmarkRecord) -> str:
    tags = "".join(
        '<span class="tag">%s</span>' % html.escape(tag) for tag in record.core_tags
    )
    search_text = " ".join(
        [record.doi, record.title, record.notice_status] + record.core_tags
    ).lower()
    return """
<article class="record-card" data-search="%s">
  <p class="status">%s</p>
  <h2><a href="records/%s.html">%s</a></h2>
  <p class="meta">%s | %s | %s</p>
  <p>%s</p>
  <div class="tags">%s</div>
</article>
""" % (
        html.escape(search_text),
        html.escape(record.notice_status.replace("_", " ")),
        html.escape(slugify(record.doi)),
        html.escape(record.title),
        html.escape(record.doi),
        html.escape(str(record.publication_year)),
        html.escape(record.subfield),
        html.escape(record.public_summary),
        tags,
    )


def _record_page(record: BenchmarkRecord) -> str:
    public_provenance = [
        entry for entry in record.provenance if entry.publicly_visible
    ]
    source_list = "".join(_source_item(entry) for entry in public_provenance)
    tags = "".join(
        '<span class="tag">%s</span>' % html.escape(tag) for tag in record.core_tags
    )
    author_text = ", ".join(record.authors)
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>%s</title>
  <link rel="stylesheet" href="../styles.css">
</head>
<body>
  <div class="shell shell-narrow">
    <p><a href="../index.html">Back to index</a></p>
    <p class="eyebrow">%s</p>
    <h1>%s</h1>
    <p class="lede">%s</p>
    <div class="hero-meta">
      <span>%s</span>
      <span>%s</span>
      <span>Snapshot %s</span>
    </div>
    <div class="tags">%s</div>
    <section class="panel">
      <h2>Paper metadata</h2>
      <p><strong>DOI:</strong> %s</p>
      <p><strong>Authors:</strong> %s</p>
      <p><strong>Venue:</strong> %s</p>
      <p><strong>Publisher:</strong> %s</p>
      <p><strong>Publication date:</strong> %s</p>
    </section>
    <section class="panel">
      <h2>Evidence summary</h2>
      <p>%s</p>
      <p><strong>Disclaimer:</strong> %s</p>
      <ul>%s</ul>
    </section>
  </div>
</body>
</html>
""" % (
        html.escape(record.title),
        html.escape(record.notice_status.replace("_", " ")),
        html.escape(record.title),
        html.escape(record.abstract),
        html.escape(record.subfield),
        html.escape(record.notice_status.replace("_", " ")),
        html.escape(record.snapshot_date),
        tags,
        html.escape(record.doi),
        html.escape(author_text),
        html.escape(record.venue),
        html.escape(record.publisher),
        html.escape(record.publication_date),
        html.escape(record.public_summary),
        html.escape(SITE_DISCLAIMER),
        source_list,
    )


def _source_item(entry: SourceProvenance) -> str:
    return '<li><a href="%s">%s</a> - %s</li>' % (
        html.escape(entry.source_url),
        html.escape(entry.source_name),
        html.escape(entry.summary),
    )


def _policy_page(summary: dict) -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Policy</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <div class="shell shell-narrow">
    <p><a href="index.html">Back to index</a></p>
    <p class="eyebrow">Policy and governance</p>
    <h1>Publication policy</h1>
    <section class="panel">
      <p>%s</p>
      <ul>
        <li>Official notices may be published automatically with source-linked factual summaries.</li>
        <li>Papers with external signals but no official notices are kept out of the public site until curator review.</li>
        <li>The site never shows numeric risk scores, unsupported allegations, or leaderboards.</li>
        <li>Extension signals remain internal and link-only unless redistribution rights are explicit and curator review is complete.</li>
      </ul>
      <p><strong>Snapshot:</strong> %s</p>
      <p><strong>Dispute and correction contact:</strong> %s</p>
    </section>
  </div>
</body>
</html>
""" % (
        html.escape(SITE_DISCLAIMER),
        html.escape(summary["snapshot_date"]),
        html.escape(POLICY_CONTACT),
    )


def _changes_page(summary: dict) -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Change log</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <div class="shell shell-narrow">
    <p><a href="index.html">Back to index</a></p>
    <p class="eyebrow">Release notes</p>
    <h1>Change log</h1>
    <section class="panel">
      <p><strong>v0.2.0</strong> - Leakage-safe benchmark scaffold with internal-only curator queue.</p>
      <ul>
        <li>Added feature cutoff tracking and stronger leakage audit.</li>
        <li>Added explicit grouped holdout split manifests.</li>
        <li>Moved curator queue out of the public site output.</li>
      </ul>
      <p><strong>Snapshot:</strong> %s</p>
      <p><strong>Public records:</strong> %s</p>
    </section>
  </div>
</body>
</html>
""" % (html.escape(summary["snapshot_date"]), summary["auto_publish_count"])


def _styles_css() -> str:
    return """
:root {
  --bg: #f6f1e7;
  --paper: #fffaf2;
  --ink: #1d2a26;
  --muted: #5a685f;
  --accent: #2d6f55;
  --accent-soft: #d8eadf;
  --border: #d2c7b3;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: "Avenir Next", "Trebuchet MS", sans-serif;
  color: var(--ink);
  background:
    radial-gradient(circle at top left, #fff8ee 0%, transparent 45%),
    linear-gradient(180deg, #f2ebdf 0%, var(--bg) 45%, #efe5d5 100%);
}
a { color: var(--accent); }
.shell {
  max-width: 1080px;
  margin: 0 auto;
  padding: 32px 20px 56px;
}
.shell-narrow { max-width: 760px; }
.hero, .panel, .record-card, .search-panel {
  background: rgba(255, 250, 242, 0.92);
  border: 1px solid var(--border);
  border-radius: 18px;
  box-shadow: 0 12px 30px rgba(49, 58, 49, 0.08);
}
.hero {
  padding: 28px;
  margin-bottom: 24px;
}
.eyebrow {
  text-transform: uppercase;
  letter-spacing: 0.14em;
  font-size: 0.78rem;
  color: var(--muted);
}
.lede {
  font-size: 1.08rem;
  line-height: 1.6;
  color: var(--muted);
}
.hero-meta, .meta {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  color: var(--muted);
  font-size: 0.92rem;
}
.search-panel {
  padding: 18px 20px;
  margin-bottom: 24px;
}
label {
  display: block;
  margin-bottom: 10px;
  font-weight: 600;
}
input[type="search"] {
  width: 100%;
  border-radius: 12px;
  border: 1px solid var(--border);
  padding: 14px 16px;
  font-size: 1rem;
  background: white;
}
.cards {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
}
.record-card, .panel {
  padding: 20px;
}
.status {
  margin: 0 0 10px;
  font-weight: 700;
  color: var(--accent);
}
.tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 16px;
}
.tag {
  padding: 6px 10px;
  border-radius: 999px;
  background: var(--accent-soft);
  font-size: 0.85rem;
}
ul {
  line-height: 1.7;
}
@media (max-width: 700px) {
  .shell { padding: 20px 14px 40px; }
  .hero { padding: 22px; }
}
"""

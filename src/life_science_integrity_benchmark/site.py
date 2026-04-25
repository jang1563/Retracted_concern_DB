"""Static evidence-browser generator."""

import html
import json
from datetime import date
from pathlib import Path
from typing import List
from urllib.parse import urlparse

from .constants import POLICY_CONTACT, SITE_DISCLAIMER
from .types import BenchmarkRecord, SourceProvenance
from .utils import slugify, write_json


def build_site(records: List[BenchmarkRecord], output_dir: Path, summary: dict) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    site_summary = dict(summary)
    site_summary.setdefault("site_generated_date", date.today().isoformat())
    stale_internal_file = output_dir / "curation_queue.html"
    if stale_internal_file.exists():
        stale_internal_file.unlink()
    public_records = [record for record in records if record.auto_publish]

    record_dir = output_dir / "records"
    record_dir.mkdir(parents=True, exist_ok=True)
    for stale_page in record_dir.glob("*.html"):
        stale_page.unlink()

    for record in public_records:
        path = record_dir / ("%s.html" % slugify(record.doi))
        path.write_text(_record_page(record), encoding="utf-8")

    (output_dir / "styles.css").write_text(_styles_css(), encoding="utf-8")
    (output_dir / "index.html").write_text(_index_page(public_records, site_summary), encoding="utf-8")
    (output_dir / "policy.html").write_text(_policy_page(site_summary), encoding="utf-8")
    (output_dir / "changes.html").write_text(_changes_page(site_summary), encoding="utf-8")

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
    status_options = _status_options(records)
    stats = "".join(
        [
            _summary_stat("Data snapshot", summary.get("snapshot_date", "unknown")),
            _summary_stat("Site updated", summary.get("site_generated_date", "unknown")),
            _summary_stat("Public records", len(records)),
            _summary_stat("Curator-gated", summary.get("curated_review_count", 0)),
        ]
    )
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
  <div class="site-shell">
    %s
    <header class="hero">
      <div>
        <p class="eyebrow">Read-only evidence browser</p>
        <h1>Life-Science Integrity Signals</h1>
        <p class="lede">%s</p>
      </div>
      <div class="hero-note">
        <strong>Public display policy</strong>
        <span>Official notices are linked and factual. Non-notice external signals stay curator-gated.</span>
      </div>
    </header>
    <section class="summary-grid" aria-label="Release summary">
      %s
    </section>
    <section class="browser-toolbar" aria-label="Record controls">
      <div class="field field-search">
        <label for="search">Search records</label>
        <input id="search" type="search" placeholder="DOI, title, subfield, or tag">
      </div>
      <div class="field field-filter">
        <label for="status-filter">Status</label>
        <select id="status-filter">
          <option value="">All public statuses</option>
          %s
        </select>
      </div>
    </section>
    <section id="cards" class="record-list" aria-label="Public records">
      %s
    </section>
    <p id="empty-state" class="empty-state" hidden>No public records match the current filters.</p>
  </div>
  <script id="records-data" type="application/json">%s</script>
  <script>
    const input = document.getElementById('search');
    const status = document.getElementById('status-filter');
    const empty = document.getElementById('empty-state');
    const cards = Array.from(document.querySelectorAll('.record-card'));
    const applyFilters = () => {
      const query = input.value.toLowerCase().trim();
      const selectedStatus = status.value;
      let visibleCount = 0;
      cards.forEach((card) => {
        const matchesQuery = !query || card.dataset.search.includes(query);
        const matchesStatus = !selectedStatus || card.dataset.status === selectedStatus;
        const showCard = matchesQuery && matchesStatus;
        card.hidden = !showCard;
        if (showCard) visibleCount += 1;
      });
      empty.hidden = visibleCount !== 0;
    };
    input.addEventListener('input', applyFilters);
    status.addEventListener('change', applyFilters);
    applyFilters();
  </script>
</body>
</html>
""" % (
        _site_nav(current="records"),
        html.escape(SITE_DISCLAIMER),
        stats,
        status_options,
        cards,
        search_blob,
    )


def _record_card(record: BenchmarkRecord) -> str:
    tags = "".join(
        '<span class="tag">%s</span>' % html.escape(tag) for tag in record.core_tags
    )
    status_label = _status_label(record.notice_status)
    status_class = _status_class(record.notice_status)
    search_text = " ".join(
        [
            record.doi,
            record.title,
            record.notice_status,
            record.subfield,
        ]
        + record.core_tags
    ).lower()
    return """
<article class="record-card" data-status="%s" data-search="%s">
  <div class="record-card-top">
    <p class="status %s">%s</p>
    <p class="record-year">%s</p>
  </div>
  <h2><a href="records/%s.html">%s</a></h2>
  <p class="record-summary">%s</p>
  <dl class="record-meta">
    <div>
      <dt>DOI</dt>
      <dd>%s</dd>
    </div>
    <div>
      <dt>Subfield</dt>
      <dd>%s</dd>
    </div>
  </dl>
  <div class="tags">%s</div>
</article>
""" % (
        html.escape(record.notice_status),
        html.escape(search_text),
        html.escape(status_class),
        html.escape(status_label),
        html.escape(str(record.publication_year)),
        html.escape(slugify(record.doi)),
        html.escape(record.title),
        html.escape(record.public_summary),
        html.escape(record.doi),
        html.escape(record.subfield),
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
    status_label = _status_label(record.notice_status)
    status_class = _status_class(record.notice_status)
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>%s</title>
  <link rel="stylesheet" href="../styles.css">
</head>
<body>
  <div class="site-shell site-shell-narrow">
    %s
    <p class="back-link"><a href="../index.html">Back to records</a></p>
    <header class="record-header">
      <p class="status %s">%s</p>
      <h1>%s</h1>
      <p class="lede">%s</p>
      <div class="record-facts">
        <span>%s</span>
        <span>Data snapshot %s</span>
        <span>%s</span>
      </div>
      <div class="tags">%s</div>
    </header>
    <section class="panel">
      <h2>Paper metadata</h2>
      <dl class="detail-list">
        <div>
          <dt>DOI</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Authors</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Venue</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Publisher</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Publication date</dt>
          <dd>%s</dd>
        </div>
      </dl>
    </section>
    <section class="panel">
      <h2>Evidence summary</h2>
      <p>%s</p>
      <p class="disclaimer">%s</p>
      <ul class="source-list">%s</ul>
    </section>
  </div>
</body>
</html>
""" % (
        html.escape(record.title),
        _site_nav(prefix="../", current="records"),
        html.escape(status_class),
        html.escape(status_label),
        html.escape(record.title),
        html.escape(record.abstract),
        html.escape(record.subfield),
        html.escape(record.snapshot_date),
        html.escape(str(record.publication_year)),
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
    return '<li class="source-item"><a href="%s">%s</a><p>%s</p></li>' % (
        html.escape(_safe_href(entry.source_url)),
        html.escape(entry.source_name),
        html.escape(entry.summary),
    )


def _safe_href(value: str) -> str:
    parsed = urlparse(value or "")
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value
    return "#"


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
  <div class="site-shell site-shell-narrow">
    %s
    <header class="page-header">
      <p class="eyebrow">Policy and governance</p>
      <h1>Publication policy</h1>
      <p class="lede">Public pages are intentionally conservative: source-linked notices are shown, while weaker external signals remain internal until human review.</p>
    </header>
    <section class="panel">
      <p class="disclaimer">%s</p>
      <ul>
        <li>Official notices may be published automatically with source-linked factual summaries.</li>
        <li>Papers with external signals but no official notices are kept out of the public site until curator review.</li>
        <li>The site never shows numeric risk scores, unsupported allegations, or leaderboards.</li>
        <li>Extension signals remain internal and link-only unless redistribution rights are explicit and curator review is complete.</li>
      </ul>
      <dl class="detail-list compact">
        <div>
          <dt>Data snapshot</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Site updated</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Dispute and correction contact</dt>
          <dd>%s</dd>
        </div>
      </dl>
    </section>
  </div>
</body>
</html>
""" % (
        _site_nav(current="policy"),
        html.escape(SITE_DISCLAIMER),
        html.escape(summary["snapshot_date"]),
        html.escape(summary["site_generated_date"]),
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
  <div class="site-shell site-shell-narrow">
    %s
    <header class="page-header">
      <p class="eyebrow">Release notes</p>
      <h1>Change log</h1>
      <p class="lede">The live browser is rebuilt from the repository workflow and keeps data currency separate from site presentation.</p>
    </header>
    <section class="panel">
      <h2>Live site refresh</h2>
      <p>Updated the public evidence browser with a denser professional layout, status filtering, responsive record pages, and a clearer release summary.</p>
      <h2>v0.2 protocol scaffold</h2>
      <p>Leakage-safe benchmark scaffold with internal-only curator queue.</p>
      <ul>
        <li>Added feature cutoff tracking and stronger leakage audit.</li>
        <li>Added explicit grouped holdout split manifests.</li>
        <li>Moved curator queue out of the public site output.</li>
      </ul>
      <dl class="detail-list compact">
        <div>
          <dt>Data snapshot</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Site updated</dt>
          <dd>%s</dd>
        </div>
        <div>
          <dt>Public records</dt>
          <dd>%s</dd>
        </div>
      </dl>
    </section>
  </div>
</body>
</html>
""" % (
        _site_nav(current="changes"),
        html.escape(summary["snapshot_date"]),
        html.escape(summary["site_generated_date"]),
        summary["auto_publish_count"],
    )


def _site_nav(prefix: str = "", current: str = "records") -> str:
    links = [
        ("records", "Records", "index.html"),
        ("policy", "Policy", "policy.html"),
        ("changes", "Change log", "changes.html"),
    ]
    nav_links = []
    for key, label, href in links:
        current_attr = ' aria-current="page"' if key == current else ""
        nav_links.append(
            '<a href="%s"%s>%s</a>'
            % (html.escape(prefix + href), current_attr, html.escape(label))
        )
    return """
<nav class="site-nav" aria-label="Primary">
  <a class="brand" href="%s">LSIB</a>
  <div class="nav-links">%s</div>
</nav>
""" % (
        html.escape(prefix + "index.html"),
        "".join(nav_links),
    )


def _status_options(records: List[BenchmarkRecord]) -> str:
    statuses = []
    seen = set()
    for record in records:
        if record.notice_status not in seen:
            seen.add(record.notice_status)
            statuses.append(record.notice_status)
    return "\n".join(
        '<option value="%s">%s</option>' % (html.escape(status), html.escape(_status_label(status)))
        for status in sorted(statuses)
    )


def _summary_stat(label: str, value) -> str:
    return """
<div class="summary-stat">
  <span>%s</span>
  <strong>%s</strong>
</div>
""" % (
        html.escape(label),
        html.escape(_display_value(value)),
    )


def _display_value(value) -> str:
    if isinstance(value, int):
        return "{:,}".format(value)
    return str(value)


def _status_label(value: str) -> str:
    return (value or "unknown").replace("_", " ").title()


def _status_class(value: str) -> str:
    return "status-" + slugify(value or "unknown")


def _styles_css() -> str:
    return """
:root {
  --bg: #f7f8f5;
  --surface: #ffffff;
  --surface-soft: #eef4f2;
  --ink: #18211f;
  --muted: #5d6a65;
  --accent: #0b6b5f;
  --accent-strong: #064d45;
  --warning: #a45d00;
  --danger: #a1413d;
  --border: #d9e1dd;
  --border-strong: #b9c7c1;
  --shadow: 0 16px 40px rgba(26, 32, 29, 0.08);
}
* { box-sizing: border-box; }
html { color-scheme: light; }
body {
  margin: 0;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: var(--ink);
  background: linear-gradient(180deg, #ffffff 0, var(--bg) 44%, #f3f0e9 100%);
}
a {
  color: var(--accent);
  text-decoration-thickness: 1px;
  text-underline-offset: 3px;
}
a:hover { color: var(--accent-strong); }
a:focus-visible,
button:focus-visible,
input:focus-visible,
select:focus-visible {
  outline: 3px solid rgba(11, 107, 95, 0.24);
  outline-offset: 2px;
}
.site-shell {
  max-width: 1120px;
  margin: 0 auto;
  padding: 26px 22px 60px;
}
.site-shell-narrow { max-width: 820px; }
.site-nav {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 10px 0 26px;
}
.brand {
  display: inline-flex;
  align-items: center;
  min-width: 44px;
  min-height: 44px;
  justify-content: center;
  border: 1px solid var(--border-strong);
  border-radius: 8px;
  color: var(--ink);
  font-weight: 800;
  text-decoration: none;
}
.nav-links {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}
.nav-links a {
  border-radius: 8px;
  color: var(--muted);
  font-size: 0.94rem;
  font-weight: 700;
  padding: 10px 12px;
  text-decoration: none;
}
.nav-links a[aria-current="page"] {
  background: var(--surface-soft);
  color: var(--accent-strong);
}
.hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: 28px;
  align-items: end;
  border-bottom: 1px solid var(--border);
  padding: 28px 0 30px;
}
.eyebrow {
  color: var(--accent-strong);
  font-size: 0.82rem;
  font-weight: 800;
  letter-spacing: 0;
  margin: 0 0 10px;
  text-transform: uppercase;
}
h1,
h2,
p {
  margin-top: 0;
}
h1 {
  font-size: 2.55rem;
  line-height: 1.05;
  margin-bottom: 14px;
}
h2 {
  font-size: 1.12rem;
  line-height: 1.3;
}
.lede {
  color: var(--muted);
  font-size: 1.05rem;
  line-height: 1.65;
  margin-bottom: 0;
}
.hero-note {
  background: var(--surface);
  border: 1px solid var(--border);
  border-left: 4px solid var(--accent);
  border-radius: 8px;
  box-shadow: var(--shadow);
  display: grid;
  gap: 8px;
  padding: 18px;
}
.hero-note span {
  color: var(--muted);
  line-height: 1.5;
}
.summary-grid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 22px 0;
}
.summary-stat {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
}
.summary-stat span {
  color: var(--muted);
  display: block;
  font-size: 0.82rem;
  font-weight: 700;
  margin-bottom: 8px;
}
.summary-stat strong {
  display: block;
  font-size: 1.35rem;
  line-height: 1.2;
}
.browser-toolbar {
  align-items: end;
  background: rgba(255, 255, 255, 0.86);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: var(--shadow);
  display: grid;
  gap: 14px;
  grid-template-columns: minmax(0, 1fr) 230px;
  margin: 0 0 18px;
  padding: 16px;
}
label {
  color: var(--ink);
  display: block;
  font-size: 0.88rem;
  font-weight: 800;
  margin-bottom: 8px;
}
input[type="search"],
select {
  background: #ffffff;
  border: 1px solid var(--border-strong);
  border-radius: 8px;
  color: var(--ink);
  font: inherit;
  min-height: 44px;
  padding: 10px 12px;
  width: 100%;
}
.record-list {
  display: grid;
  gap: 12px;
}
.record-card,
.panel {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.record-card {
  display: grid;
  gap: 14px;
  padding: 18px;
}
.record-card-top,
.record-facts {
  align-items: center;
  color: var(--muted);
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.record-card h2 {
  font-size: 1.18rem;
  margin-bottom: 0;
}
.record-card h2 a {
  color: var(--ink);
  text-decoration-color: rgba(11, 107, 95, 0.35);
}
.record-summary {
  color: var(--muted);
  line-height: 1.55;
  margin-bottom: 0;
}
.record-year {
  color: var(--muted);
  font-size: 0.9rem;
  font-weight: 700;
  margin: 0;
}
.status {
  align-items: center;
  border-radius: 999px;
  display: inline-flex;
  font-size: 0.78rem;
  font-weight: 800;
  line-height: 1;
  margin: 0;
  padding: 7px 9px;
}
.status-retracted {
  background: #f7e7e5;
  color: var(--danger);
}
.status-editorial-notice {
  background: #fff1d6;
  color: var(--warning);
}
.status-unknown {
  background: var(--surface-soft);
  color: var(--muted);
}
.record-meta,
.detail-list {
  display: grid;
  gap: 10px;
  margin: 0;
}
.record-meta {
  grid-template-columns: minmax(0, 1fr) 180px;
}
.record-meta div,
.detail-list div {
  min-width: 0;
}
dt {
  color: var(--muted);
  font-size: 0.78rem;
  font-weight: 800;
  margin-bottom: 3px;
  text-transform: uppercase;
}
dd {
  margin: 0;
  overflow-wrap: anywhere;
}
.tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.tag {
  background: var(--surface-soft);
  border: 1px solid #cfddd7;
  border-radius: 999px;
  color: var(--accent-strong);
  font-size: 0.82rem;
  font-weight: 700;
  padding: 6px 9px;
}
.empty-state {
  background: var(--surface);
  border: 1px dashed var(--border-strong);
  border-radius: 8px;
  color: var(--muted);
  margin: 14px 0 0;
  padding: 18px;
  text-align: center;
}
.back-link {
  margin: 0 0 18px;
}
.record-header,
.page-header {
  border-bottom: 1px solid var(--border);
  margin-bottom: 22px;
  padding-bottom: 24px;
}
.record-header h1,
.page-header h1 {
  font-size: 2.05rem;
}
.record-header .status {
  margin-bottom: 14px;
}
.record-facts {
  margin-top: 16px;
}
.record-facts span {
  background: var(--surface-soft);
  border-radius: 8px;
  color: var(--ink);
  font-size: 0.9rem;
  font-weight: 700;
  padding: 8px 10px;
}
.record-header .tags {
  margin-top: 16px;
}
.panel {
  margin-top: 16px;
  padding: 20px;
}
.panel h2 + p {
  color: var(--muted);
  line-height: 1.6;
}
.detail-list {
  grid-template-columns: 1fr;
}
.detail-list div {
  border-top: 1px solid var(--border);
  padding-top: 10px;
}
.detail-list div:first-child {
  border-top: 0;
  padding-top: 0;
}
.detail-list.compact {
  margin-top: 18px;
}
.disclaimer {
  background: #f6f0e6;
  border-left: 4px solid var(--warning);
  border-radius: 8px;
  color: #58483a;
  line-height: 1.55;
  padding: 14px 16px;
}
.source-list {
  display: grid;
  gap: 10px;
  list-style: none;
  margin: 16px 0 0;
  padding: 0;
}
.source-item {
  border-top: 1px solid var(--border);
  padding-top: 12px;
}
.source-item:first-child {
  border-top: 0;
  padding-top: 0;
}
.source-item a {
  font-weight: 800;
}
.source-item p {
  color: var(--muted);
  line-height: 1.55;
  margin: 6px 0 0;
}
ul {
  line-height: 1.7;
}
@media (max-width: 820px) {
  .site-shell {
    padding: 18px 14px 42px;
  }
  .site-nav,
  .hero {
    align-items: stretch;
    grid-template-columns: 1fr;
  }
  .site-nav {
    flex-direction: column;
  }
  .brand {
    align-self: flex-start;
  }
  .nav-links {
    width: 100%;
  }
  .nav-links a {
    flex: 1 1 auto;
    text-align: center;
  }
  h1 {
    font-size: 2.05rem;
  }
  .summary-grid,
  .browser-toolbar,
  .record-meta {
    grid-template-columns: 1fr;
  }
}
@media (max-width: 460px) {
  h1,
  .record-header h1,
  .page-header h1 {
    font-size: 1.72rem;
  }
  .hero-note,
  .summary-stat,
  .browser-toolbar,
  .record-card,
  .panel {
    padding: 14px;
  }
}
"""

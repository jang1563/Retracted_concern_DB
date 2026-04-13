#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <vendor_archive_root> <snapshot_label>" >&2
  exit 1
fi

VENDOR_ROOT="$1"
SNAPSHOT_LABEL="$2"
PERIOD_KEY="${SNAPSHOT_LABEL%-freeze}"
YEAR="${PERIOD_KEY%-*}"
BASELINE_DIR="$VENDOR_ROOT/pubmed/baseline/$YEAR"
UPDATE_DIR="$VENDOR_ROOT/pubmed/updatefiles/$PERIOD_KEY"
WGET_BIN="${WGET_BIN:-wget}"
CURL_BIN="${CURL_BIN:-curl}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$BASELINE_DIR" "$UPDATE_DIR"

download_listing_files() {
  local listing_url="$1"
  local target_dir="$2"
  local listing_file=""
  local link_file=""
  listing_file="$(mktemp /tmp/pubmed-listing.XXXXXX.html)"
  link_file="$(mktemp /tmp/pubmed-links.XXXXXX.tsv)"
  if [ -x "$CURL_BIN" ] || command -v "$CURL_BIN" >/dev/null 2>&1; then
    "$CURL_BIN" -fsSLk "$listing_url" > "$listing_file"
  else
    "$WGET_BIN" -qO "$listing_file" --no-check-certificate "$listing_url"
  fi
  "$PYTHON_BIN" - "$listing_url" "$listing_file" <<'PY' > "$link_file"
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin
import sys

listing_url = sys.argv[1]
listing_path = Path(sys.argv[2])

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)

html = listing_path.read_text(encoding="utf-8", errors="ignore")

parser = LinkParser()
parser.feed(html)
seen = set()
for href in parser.links:
    name = href.rsplit("/", 1)[-1]
    if not name or name in seen:
        continue
    if name.endswith(".xml.gz") or name == "README.txt":
        seen.add(name)
        print(f"{urljoin(listing_url, href)}\t{name}")
PY
  while IFS=$'\t' read -r file_url file_name; do
    [ -n "$file_url" ] || continue
    tmp_path="$target_dir/$file_name.tmp"
    final_path="$target_dir/$file_name"
    "$WGET_BIN" -q --no-check-certificate -O "$tmp_path" "$file_url"
    mv "$tmp_path" "$final_path"
  done < "$link_file"
  rm -f "$listing_file" "$link_file"
}

download_listing_files "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/" "$BASELINE_DIR"
download_listing_files "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/" "$UPDATE_DIR"

find "$BASELINE_DIR" -maxdepth 1 \( -name 'robots.txt*' -o -name 'index.html*' \) -delete 2>/dev/null || true
find "$UPDATE_DIR" -maxdepth 1 \( -name 'robots.txt*' -o -name 'index.html*' \) -delete 2>/dev/null || true

"$PYTHON_BIN" - "$BASELINE_DIR" "$UPDATE_DIR" "$SNAPSHOT_LABEL" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

baseline_dir = Path(sys.argv[1])
update_dir = Path(sys.argv[2])
snapshot_label = sys.argv[3]
payload = {
    "snapshot_label": snapshot_label,
    "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "batches": [
        {
            "source_name": "PubMed baseline",
            "source_url": "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
        },
        {
            "source_name": "PubMed updatefiles",
            "source_url": "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/",
        },
    ],
}
for target_dir in (baseline_dir, update_dir):
    (target_dir / "source_versions.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY

for target_dir in "$BASELINE_DIR" "$UPDATE_DIR"; do
  {
    printf "relative_path\tsize_bytes\tsha256\n"
    find "$target_dir" -type f ! -name 'sha256_manifest.tsv' | sort | while IFS= read -r file_path; do
      rel_path="${file_path#$target_dir/}"
      size_bytes="$(wc -c < "$file_path" | tr -d ' ')"
      sha256="$(shasum -a 256 "$file_path" | awk '{print $1}')"
      printf "%s\t%s\t%s\n" "$rel_path" "$size_bytes" "$sha256"
    done
  } > "$target_dir/sha256_manifest.tsv"
done

echo "$BASELINE_DIR"
echo "$UPDATE_DIR"

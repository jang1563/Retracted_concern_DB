#!/bin/bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <mixed_source_root> [output_tsv]" >&2
  exit 1
fi

SOURCE_ROOT="$1"
OUTPUT_PATH="${2:-$SOURCE_ROOT/source_classification.tsv}"

if [ ! -d "$SOURCE_ROOT" ]; then
  echo "mixed source root does not exist: $SOURCE_ROOT" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
printf "bucket\treason\trelative_path\n" > "$OUTPUT_PATH"

should_skip_generated_file() {
  local abs_path="$1"
  local rel_path="$2"
  local base_name
  abs_path="$(cd "$(dirname "$abs_path")" && pwd)/$(basename "$abs_path")"
  base_name="$(basename "$rel_path")"

  if [ "$abs_path" = "$(cd "$(dirname "$OUTPUT_PATH")" && pwd)/$(basename "$OUTPUT_PATH")" ]; then
    return 0
  fi

  case "$base_name" in
    source_classification.tsv|source_classification.merged.tsv|classification_overrides.tsv)
      return 0
      ;;
  esac

  return 1
}

sample_file_text() {
  local file_path="$1"
  case "$file_path" in
    *.gz)
      gzip -cd "$file_path" 2>/dev/null | sed -n '1,5p'
      ;;
    *)
      sed -n '1,5p' "$file_path" 2>/dev/null
      ;;
  esac
}

classify_path() {
  local rel_path="$1"
  local abs_path="$2"
  local lc_path
  local sample
  local lc_sample
  lc_path="$(printf "%s" "$rel_path" | tr '[:upper:]' '[:lower:]')"
  sample="$(sample_file_text "$abs_path" | tr '\n' ' ' | tr '\r' ' ')"
  lc_sample="$(printf "%s" "$sample" | tr '[:upper:]' '[:lower:]')"

  case "$lc_path" in
    *.xml|*.xml.gz)
      if [[ "$lc_sample" == *pubmedarticle* ]] || [[ "$lc_sample" == *pubmedarticleset* ]] || [[ "$lc_sample" == *medlinecitation* ]]; then
        echo "pubmed	pubmed_xml_content"
        return
      fi
      echo "unknown	xml_without_pubmed_signature"
      return
      ;;
  esac

  if [[ "$lc_sample" == *"\"authorships\""* ]] || [[ "$lc_sample" == *"\"abstract_inverted_index\""* ]] || [[ "$lc_sample" == *"\"primary_location\""* ]] || [[ "$lc_sample" == *"\"type_crossref\""* ]] || [[ "$lc_sample" == *"\"referenced_works_count\""* ]] || [[ "$lc_sample" == *"\"concepts\""* ]]; then
    echo "openalex	openalex_content"
    return
  fi

  if [[ "$lc_sample" == *"\"notice_type\""* ]] || [[ "$lc_sample" == *"\"notice_label\""* ]] || [[ "$lc_sample" == *"\"update-to\""* ]] || [[ "$lc_sample" == *"\"update_to\""* ]] || [[ "$lc_sample" == *"\"relation\""* ]] || [[ "$lc_sample" == *"\"source_url\""* ]]; then
    echo "official_notices	notice_content"
    return
  fi

  if [[ "$lc_sample" == *"\"pmid\""* ]] || [[ "$lc_sample" == *"\"pubmed_id\""* ]] || [[ "$lc_sample" == *"\"mesh_terms\""* ]] || [[ "$lc_sample" == *"\"mesh_headings\""* ]] || [[ "$lc_sample" == *"\"publication_types\""* ]] || [[ "$lc_sample" == *"\"journal_title\""* ]]; then
    echo "pubmed	pubmed_content"
    return
  fi

  if [[ "$lc_sample" == doi,*pmid* ]] || [[ "$lc_sample" == pmid,*doi* ]] || [[ "$lc_sample" == *mesh_terms* ]] || [[ "$lc_sample" == *mesh_headings* ]] || [[ "$lc_sample" == *publication_types* ]]; then
    echo "pubmed	pubmed_csv_header"
    return
  fi

  if [[ "$lc_sample" == doi,*notice_type* ]] || [[ "$lc_sample" == doi,*notice_label* ]] || [[ "$lc_sample" == doi,*tag* ]] || [[ "$lc_sample" == *source_url* ]] || [[ "$lc_sample" == *source_name* ]]; then
    echo "official_notices	notice_csv_header"
    return
  fi

  if [[ "$lc_sample" == doi,*title* ]] && [[ "$lc_sample" == *publication* ]] && [[ "$lc_sample" == *abstract* ]]; then
    echo "openalex	openalex_csv_header"
    return
  fi

  if [[ "$lc_path" == *pubmed* ]] || [[ "$lc_path" == *pmid* ]] || [[ "$lc_path" == *mesh* ]] || [[ "$lc_path" == *medline* ]]; then
    echo "pubmed	pubmed_keyword"
    return
  fi

  if [[ "$lc_path" == *openalex* ]] || [[ "$lc_path" == *works* ]]; then
    echo "openalex	openalex_keyword"
    return
  fi

  if [[ "$lc_path" == *crossmark* ]] || [[ "$lc_path" == *crossref* ]] || [[ "$lc_path" == *retraction* ]] || [[ "$lc_path" == *notice* ]] || [[ "$lc_path" == *correction* ]] || [[ "$lc_path" == *concern* ]] || [[ "$lc_path" == *update* ]]; then
    echo "official_notices	notice_keyword"
    return
  fi

  case "$lc_path" in
    *.jsonl|*.jsonl.gz)
      echo "unknown	jsonl_ambiguous"
      return
      ;;
    *.csv|*.csv.gz)
      echo "unknown	csv_ambiguous"
      return
      ;;
  esac

  echo "unknown	unsupported_or_ambiguous"
}

while IFS= read -r file_path; do
  [ -n "$file_path" ] || continue
  rel_path="${file_path#$SOURCE_ROOT/}"
  if should_skip_generated_file "$file_path" "$rel_path"; then
    continue
  fi
  classification="$(classify_path "$rel_path" "$file_path")"
  bucket="${classification%%	*}"
  reason="${classification#*	}"
  printf "%s\t%s\t%s\n" "$bucket" "$reason" "$rel_path" >> "$OUTPUT_PATH"
done < <(find "$SOURCE_ROOT" -type f | sort)

echo "$OUTPUT_PATH"

#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE_DIR="$SCRIPT_DIR/templates"
JOB_DIR="$RUN_ROOT/jobs"

mkdir -p \
  "$RUN_ROOT/repo" \
  "$JOB_DIR" \
  "$RUN_ROOT/logs" \
  "$RUN_ROOT/raw/real_snapshot/openalex" \
  "$RUN_ROOT/raw/real_snapshot/official_notices" \
  "$RUN_ROOT/raw/real_snapshot/pubmed" \
  "$RUN_ROOT/artifacts/preflight" \
  "$RUN_ROOT/artifacts/sample_stress" \
  "$RUN_ROOT/artifacts/open_data_release" \
  "$RUN_ROOT/artifacts/open_data_site" \
  "$RUN_ROOT/artifacts/real_release" \
  "$RUN_ROOT/artifacts/real_site"

render_template() {
  local src="$1"
  local dst="$2"
  sed "s|__RUN_ROOT__|$RUN_ROOT|g" "$src" > "$dst"
}

render_template "$TEMPLATE_DIR/preflight.sbatch.in" "$JOB_DIR/preflight.sbatch"
render_template "$TEMPLATE_DIR/sample_stress.sbatch.in" "$JOB_DIR/sample_stress.sbatch"
render_template "$TEMPLATE_DIR/open_data_finalize_template.sbatch.in" "$JOB_DIR/open_data_finalize.sbatch"
render_template "$TEMPLATE_DIR/real_ingest_template.sbatch.in" "$JOB_DIR/real_ingest_template.sbatch"
render_template "$TEMPLATE_DIR/public_vendor_collection.sbatch.in" "$JOB_DIR/public_vendor_collection.sbatch"
render_template "$TEMPLATE_DIR/raw_real_snapshot_README.txt.in" "$RUN_ROOT/raw/real_snapshot/README.txt"
chmod +x "$JOB_DIR/preflight.sbatch" "$JOB_DIR/sample_stress.sbatch" "$JOB_DIR/open_data_finalize.sbatch" "$JOB_DIR/real_ingest_template.sbatch" "$JOB_DIR/public_vendor_collection.sbatch"

echo "$RUN_ROOT"
echo "Rendered jobs into $JOB_DIR"

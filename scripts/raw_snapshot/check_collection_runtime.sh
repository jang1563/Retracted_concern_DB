#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

if [ "$#" -gt 0 ]; then
  echo "usage: $0" >&2
  exit 1
fi

AWS_BIN="${AWS_BIN:-aws}"
CURL_BIN="${CURL_BIN:-curl}"
WGET_BIN="${WGET_BIN:-wget}"
GIT_BIN="${GIT_BIN:-git}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CROSSREF_SOURCE_MODE="${CROSSREF_SOURCE_MODE:-metadata_plus}"
STATUS=0

check_bin() {
  local label="$1"
  local path="$2"
  if command -v "$path" >/dev/null 2>&1; then
    echo "runtime_bin_ok	$label	$(command -v "$path")"
  elif [ -x "$path" ]; then
    echo "runtime_bin_ok	$label	$path"
  else
    echo "runtime_bin_missing	$label	$path"
    STATUS=1
  fi
}

check_bin "aws" "$AWS_BIN"
check_bin "curl" "$CURL_BIN"
check_bin "wget" "$WGET_BIN"
check_bin "git" "$GIT_BIN"
check_bin "python3" "$PYTHON_BIN"

echo "crossref_source_mode=$CROSSREF_SOURCE_MODE"

if [ "$CROSSREF_SOURCE_MODE" = "metadata_plus" ]; then
  if [ -n "${CROSSREF_PLUS_TOKEN:-}" ]; then
    echo "crossref_plus_token=present"
  else
    echo "crossref_plus_token=missing"
    STATUS=1
  fi
elif [ "$CROSSREF_SOURCE_MODE" = "skip" ] || [ "$CROSSREF_SOURCE_MODE" = "public_data_file" ]; then
  if [ -n "${CROSSREF_PLUS_TOKEN:-}" ]; then
    echo "crossref_plus_token=present_unused"
  else
    echo "crossref_plus_token=not_required"
  fi
else
  echo "crossref_source_mode_invalid=$CROSSREF_SOURCE_MODE"
  STATUS=1
fi

if [ "$STATUS" -ne 0 ] && ! command -v "$AWS_BIN" >/dev/null 2>&1 && command -v module >/dev/null 2>&1; then
  echo "runtime_hint\taws\ttry 'module load ${RAW_SNAPSHOT_AWS_MODULE:-awscli/2.2.14}'"
fi

if [ "$STATUS" -ne 0 ] && [ "$CROSSREF_SOURCE_MODE" = "metadata_plus" ] && [ -z "${CROSSREF_PLUS_TOKEN:-}" ] && [ -n "${RAW_SNAPSHOT_ENV_FILE:-}" ]; then
  echo "runtime_hint\tcrossref\tcheck CROSSREF_PLUS_TOKEN in \$RAW_SNAPSHOT_ENV_FILE"
fi

if [ "$STATUS" -ne 0 ]; then
  echo "collection_runtime_ready=no"
  exit 1
fi

echo "collection_runtime_ready=yes"

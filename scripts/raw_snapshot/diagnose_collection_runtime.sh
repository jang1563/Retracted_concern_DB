#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/common_collection_env.sh"

echo "hostname=$(hostname)"
echo "cwd=$(pwd)"
echo "raw_snapshot_env_file=${RAW_SNAPSHOT_ENV_FILE:-unset}"

for candidate in \
  /usr/share/lmod/lmod/init/bash \
  /usr/share/Modules/init/bash \
  /etc/profile.d/modules.sh
do
  if [ -f "$candidate" ]; then
    echo "module_init_present=$candidate"
  fi
done

if command -v module >/dev/null 2>&1; then
  echo "module_command=present"
else
  echo "module_command=missing"
fi

echo "aws_bin=${AWS_BIN:-aws}"
if command -v "${AWS_BIN:-aws}" >/dev/null 2>&1; then
  echo "aws_path=$(command -v "${AWS_BIN:-aws}")"
  "${AWS_BIN:-aws}" --version 2>&1 | sed -n '1,2p'
else
  echo "aws_path=missing"
fi

echo "curl_bin=${CURL_BIN:-curl}"
if command -v "${CURL_BIN:-curl}" >/dev/null 2>&1; then
  echo "curl_path=$(command -v "${CURL_BIN:-curl}")"
fi

echo "wget_bin=${WGET_BIN:-wget}"
if command -v "${WGET_BIN:-wget}" >/dev/null 2>&1; then
  echo "wget_path=$(command -v "${WGET_BIN:-wget}")"
fi

echo "git_bin=${GIT_BIN:-git}"
if command -v "${GIT_BIN:-git}" >/dev/null 2>&1; then
  echo "git_path=$(command -v "${GIT_BIN:-git}")"
fi

echo "python_bin=${PYTHON_BIN:-python3}"
if command -v "${PYTHON_BIN:-python3}" >/dev/null 2>&1; then
  echo "python_path=$(command -v "${PYTHON_BIN:-python3}")"
  "${PYTHON_BIN:-python3}" --version 2>&1 | sed -n '1,2p'
elif [ -x "${PYTHON_BIN:-python3}" ]; then
  echo "python_path=${PYTHON_BIN:-python3}"
  "${PYTHON_BIN:-python3}" --version 2>&1 | sed -n '1,2p'
else
  echo "python_path=missing"
fi

echo "crossref_source_mode=${CROSSREF_SOURCE_MODE:-metadata_plus}"
if [ -n "${CROSSREF_PLUS_TOKEN:-}" ]; then
  echo "crossref_plus_token=present"
else
  echo "crossref_plus_token=missing"
fi

#!/bin/bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <run_root>" >&2
  exit 1
fi

RUN_ROOT="$1"
REPO_ROOT="$RUN_ROOT/repo"
REPORT_PATH="$RUN_ROOT/artifacts/cayuga_runtime_check.txt"
COMMON_SH="$REPO_ROOT/scripts/cayuga/common_job_env.sh"

mkdir -p "$(dirname "$REPORT_PATH")"

activate_env_or_report() {
  if [ -f "$COMMON_SH" ]; then
    source "$COMMON_SH"
    if declare -F lsib_activate_conda_env >/dev/null 2>&1; then
      lsib_activate_conda_env python.3.12.R.4.3.3
      return $?
    fi
  fi

  if [ -f ~/.bashrc ]; then
    source ~/.bashrc
  fi
  if command -v conda >/dev/null 2>&1; then
    eval "$(conda shell.bash hook)" || true
    conda activate python.3.12.R.4.3.3
    return $?
  fi
  return 1
}

{
  echo "run_root=$RUN_ROOT"
  echo "generated_at=$(date '+%F %T')"
  echo "hostname=$(hostname)"
  echo

  echo "[commands]"
  for cmd in bash python3 ssh rsync sbatch sacct; do
    if command -v "$cmd" >/dev/null 2>&1; then
      echo "present	$cmd	$(command -v "$cmd")"
    else
      echo "missing	$cmd"
    fi
  done

  echo
  echo "[conda]"
  if [ -f "$COMMON_SH" ]; then
    echo "common_job_env=present"
  else
    echo "common_job_env=missing"
  fi
  if command -v conda >/dev/null 2>&1; then
    echo "conda_present=yes"
    if activate_env_or_report; then
      echo "conda_env=python.3.12.R.4.3.3"
      echo "python_version=$(python3 --version 2>&1)"
    else
      echo "conda_env_activation=failed"
    fi
  else
    echo "conda_present=no"
  fi

  echo
  echo "[repo]"
  if [ -d "$REPO_ROOT" ]; then
    echo "repo_present=yes"
    echo "repo_root=$REPO_ROOT"
    if [ -d "$REPO_ROOT/src/life_science_integrity_benchmark" ]; then
      cd "$REPO_ROOT"
      export PYTHONPATH=src
      python3 - <<'PY'
import importlib
import pathlib
module = importlib.import_module("life_science_integrity_benchmark.cli")
print("python_import=ok")
print("repo_cwd=%s" % pathlib.Path.cwd())
print("cli_module=%s" % module.__file__)
PY
    else
      echo "repo_python_package=missing"
    fi
  else
    echo "repo_present=no"
  fi
} | tee "$REPORT_PATH"

echo "report_path=$REPORT_PATH"

#!/bin/bash

lsib_prepend_python_bin() {
  local candidate
  for candidate in "$@"; do
    if [ -x "$candidate/python3" ]; then
      export PATH="$candidate:$PATH"
      return 0
    fi
    if [ -x "$candidate/python" ]; then
      export PATH="$candidate:$PATH"
      return 0
    fi
  done
  return 1
}

lsib_activate_slurm_client() {
  local candidate
  for candidate in \
    /opt/ohpc/pub/software/slurm/*/bin \
    /usr/bin
  do
    if [ -x "$candidate/sbatch" ]; then
      export PATH="$candidate:$PATH"
      return 0
    fi
  done
  return 1
}

lsib_source_first_existing() {
  local candidate
  for candidate in "$@"; do
    if [ -f "$candidate" ]; then
      source "$candidate"
      return 0
    fi
  done
  return 1
}

lsib_activate_conda_env() {
  local env_name="${1:-python.3.12.R.4.3.3}"
  local fallback_bins=(
    "$HOME/miniconda3/miniconda3/bin"
    "$HOME/miniconda3/bin"
    "$HOME/miniforge3/bin"
    "$HOME/mambaforge/bin"
  )

  if ! command -v conda >/dev/null 2>&1; then
    lsib_source_first_existing \
      "$HOME/miniconda3/etc/profile.d/conda.sh" \
      "$HOME/miniconda3/miniconda3/etc/profile.d/conda.sh" \
      "$HOME/miniforge3/etc/profile.d/conda.sh" \
      "$HOME/mambaforge/etc/profile.d/conda.sh" \
      >/dev/null 2>&1 || true
  fi
  if command -v conda >/dev/null 2>&1; then
    local conda_base
    conda_base="$(conda info --base 2>/dev/null || true)"
    if ! declare -F conda >/dev/null 2>&1; then
      if [ -n "$conda_base" ] && [ -f "$conda_base/etc/profile.d/conda.sh" ]; then
        source "$conda_base/etc/profile.d/conda.sh"
      else
        eval "$(conda shell.bash hook)"
      fi
    fi
    if conda activate "$env_name" >/dev/null 2>&1; then
      return 0
    fi
    if conda activate base >/dev/null 2>&1; then
      return 0
    fi
  fi

  if lsib_prepend_python_bin "${fallback_bins[@]}"; then
    if python3 - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 8) else 1)
PY
    then
      return 0
    fi
  fi

  echo "No usable conda environment or fallback Python was found" >&2
  return 1
}

lsib_mark_failure() {
  local art_root="$1"
  local step_name="$2"
  mkdir -p "$art_root"
  printf "%s\n" "$step_name" > "$art_root/failed_step.txt"
  touch "$art_root/FAILED"
}

lsib_require_nonempty_dir() {
  local required_dir="$1"
  if [ ! -d "$required_dir" ]; then
    echo "missing required directory: $required_dir" >&2
    return 1
  fi
  if [ "$(find "$required_dir" \( -type f -o -type l \) | wc -l | tr -d ' ')" -eq 0 ]; then
    echo "directory is empty: $required_dir" >&2
    return 1
  fi
}

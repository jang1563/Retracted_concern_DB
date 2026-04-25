#!/bin/bash

lsib_python_bin_resolve_path() {
  local candidate="$1"
  if [ -z "$candidate" ]; then
    return 1
  fi
  if command -v "$candidate" >/dev/null 2>&1; then
    command -v "$candidate"
    return 0
  fi
  if [ -x "$candidate" ]; then
    printf "%s\n" "$candidate"
    return 0
  fi
  return 1
}

lsib_python_bin_healthy() {
  local resolved
  resolved="$(lsib_python_bin_resolve_path "$1")" || return 1
  "$resolved" - <<'PY' >/dev/null 2>&1
import sys

raise SystemExit(0 if sys.version_info >= (3, 8) else 1)
PY
}

lsib_resolve_python_bin() {
  local requested="${1:-}"
  local candidate
  local resolved

  for candidate in \
    "$requested" \
    "${PYTHON_BIN:-}" \
    "$HOME/opt/anaconda3/bin/python" \
    "$HOME/opt/anaconda3/bin/python3" \
    "$HOME/anaconda3/bin/python" \
    "$HOME/anaconda3/bin/python3" \
    "$HOME/miniconda3/miniconda3/bin/python" \
    "$HOME/miniconda3/bin/python" \
    "$HOME/miniforge3/bin/python" \
    "$HOME/mambaforge/bin/python" \
    python3 \
    python
  do
    [ -n "$candidate" ] || continue
    resolved="$(lsib_python_bin_resolve_path "$candidate")" || continue
    if lsib_python_bin_healthy "$resolved"; then
      export PYTHON_BIN="$resolved"
      return 0
    fi
  done

  return 1
}

lsib_require_python_bin() {
  if ! lsib_resolve_python_bin "${1:-${PYTHON_BIN:-}}"; then
    echo "No usable Python interpreter found. Set PYTHON_BIN to a working Python 3.8+ executable." >&2
    return 1
  fi
}

#!/bin/bash

# Shared runtime bootstrap for raw-snapshot collection scripts.
# It keeps collection wrappers compatible with local shells and Cayuga module-based setups.

COMMON_PYTHON_ENV="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common_python_env.sh"
# shellcheck disable=SC1090
source "$COMMON_PYTHON_ENV"

if [ -n "${RAW_SNAPSHOT_ENV_FILE:-}" ] && [ -f "${RAW_SNAPSHOT_ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  source "${RAW_SNAPSHOT_ENV_FILE}"
fi

if [ -f "${HOME}/.bashrc" ]; then
  # shellcheck disable=SC1090
  source "${HOME}/.bashrc" >/dev/null 2>&1 || true
fi

if ! command -v module >/dev/null 2>&1; then
  for module_init in \
    "/usr/share/lmod/lmod/init/bash" \
    "/usr/share/Modules/init/bash" \
    "/etc/profile.d/modules.sh"
  do
    if [ -f "$module_init" ]; then
      # shellcheck disable=SC1090
      source "$module_init" >/dev/null 2>&1 || true
      command -v module >/dev/null 2>&1 && break
    fi
  done
fi

lsib_resolve_python_bin "${PYTHON_BIN:-}" >/dev/null 2>&1 || true

if command -v module >/dev/null 2>&1; then
  if [ -n "${RAW_SNAPSHOT_EXTRA_MODULES:-}" ]; then
    for module_name in ${RAW_SNAPSHOT_EXTRA_MODULES}; do
      module load "$module_name" >/dev/null 2>&1 || true
    done
  fi

  if [ "${RAW_SNAPSHOT_AUTOLOAD_AWS_MODULE:-1}" = "1" ]; then
    AWS_CANDIDATE="${AWS_BIN:-aws}"
    if ! command -v "$AWS_CANDIDATE" >/dev/null 2>&1 && [ -n "${RAW_SNAPSHOT_AWS_MODULE:-awscli/2.2.14}" ]; then
      module load "${RAW_SNAPSHOT_AWS_MODULE:-awscli/2.2.14}" >/dev/null 2>&1 || true
    fi
  fi
fi

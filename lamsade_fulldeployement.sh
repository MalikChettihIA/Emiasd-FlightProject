#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"
"${SCRIPT_DIR}/work/scripts/lamsade/lamsade_fulldeployement.sh" "$@"

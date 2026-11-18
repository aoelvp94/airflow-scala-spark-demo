#!/usr/bin/env bash
# Convenience wrapper around the Makefile targets.
# Usage: ./run.sh image|bundle|overlay|baseline|patched|all|evidence|clean
set -euo pipefail
cd "$(dirname "$0")"
target="${1:-all}"
exec make "$target"

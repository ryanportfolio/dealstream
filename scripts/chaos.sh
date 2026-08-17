#!/usr/bin/env bash
# Chaos controls for the simulated retailers. Usage:
#   scripts/chaos.sh dead <slug>      feed returns 503 on every request
#   scripts/chaos.sh degraded <slug>  2-4s latency and 10% 500s
#   scripts/chaos.sh active <slug>    back to normal
#   scripts/chaos.sh state            dump all retailer states
set -euo pipefail

FEED="${FEED:-http://127.0.0.1:8081}"

case "${1:-}" in
  dead|degraded|active)
    curl -s -X POST "$FEED/admin/retailers/${2:?slug required}/status" \
      -H 'Content-Type: application/json' \
      -d "{\"status\": \"$1\"}"
    echo
    ;;
  state)
    curl -s "$FEED/admin/state"
    echo
    ;;
  *)
    echo "usage: $0 dead|degraded|active <slug> | state" >&2
    exit 1
    ;;
esac

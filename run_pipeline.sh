#!/usr/bin/env bash
# run_pipeline.sh — Run the EatEatEat pipeline for all cities sequentially.
#
# Usage:
#   ./run_pipeline.sh                        # all cities, all stages
#   ./run_pipeline.sh --city london          # single city
#   ./run_pipeline.sh --force-search         # bypass 6-month TTL
#   ./run_pipeline.sh --stages search,enrich # specific stages only
#   ./run_pipeline.sh --dry-run              # preview without API calls
#
# All extra flags are passed through to pipeline.py.

set -euo pipefail

DOCKER="/usr/local/bin/docker"
CITIES=("mallorca" "stuttgart" "london" "koeln" "muenchen" "hamburg" "berlin")

# ── Parse args ────────────────────────────────────────────────────────────────

SINGLE_CITY=""
PASSTHROUGH=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --city)
      SINGLE_CITY="$2"
      shift 2
      ;;
    *)
      PASSTHROUGH+=("$1")
      shift
      ;;
  esac
done

if [[ -n "$SINGLE_CITY" ]]; then
  CITIES=("$SINGLE_CITY")
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

run_city() {
  local city="$1"
  log "════════════════════════════════════════"
  log "  Starting pipeline for: $city"
  log "════════════════════════════════════════"

  "$DOCKER" compose --profile pipeline run --rm pipeline \
    --city "$city" \
    --stages search,qualify \
    ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}

  log "  ✓ search + qualify done for $city"
}

# ── Per-city: search + qualify ────────────────────────────────────────────────

FAILED=()

for city in "${CITIES[@]}"; do
  if run_city "$city"; then
    :
  else
    log "  ✗ Pipeline failed for $city — continuing with next city"
    FAILED+=("$city")
  fi
done

# ── Cross-city: enrich, details, photos, etc. ─────────────────────────────────

log "════════════════════════════════════════"
log "  Starting cross-city stages"
log "════════════════════════════════════════"

"$DOCKER" compose --profile pipeline run --rm pipeline \
  --city all \
  --stages enrich,completeness,gem_qualify,critic_enrich,details,photos,website,classify,promote,curation,jina_embed,cuisine_dna,verify \
  ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}

log "════════════════════════════════════════"

# ── Summary ───────────────────────────────────────────────────────────────────

if [[ ${#FAILED[@]} -gt 0 ]]; then
  log "  ⚠ Pipeline complete with failures: ${FAILED[*]}"
  exit 1
else
  log "  ✓ Pipeline complete for all cities: ${CITIES[*]}"
fi

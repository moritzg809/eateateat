"""
mallorcaeat scraper — Step 1
Iterates over all (search_term × location) combinations, caches each
Serper Maps response, and upserts results into the restaurants table.

Usage:
    python scrape.py              # full run
    python scrape.py --dry-run    # show what would be called / skipped
"""

import logging
import sys

from config import LOCATIONS, SEARCH_TERMS
from db import get_connection, get_cached, link_search_result, save_cache, upsert_restaurant
from serper import search_maps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> None:
    combinations = [(term, loc) for loc in LOCATIONS for term in SEARCH_TERMS]
    total = len(combinations)

    logger.info("=" * 60)
    logger.info("mallorcaeat scraper — Step 1")
    logger.info("  search terms : %d", len(SEARCH_TERMS))
    logger.info("  locations    : %d", len(LOCATIONS))
    logger.info("  combinations : %d", total)
    if dry_run:
        logger.info("  MODE         : DRY RUN (no API calls)")
    logger.info("=" * 60)

    conn = get_connection()

    stats = {"api_calls": 0, "cached": 0, "restaurants": 0, "errors": 0}

    for i, (term, location) in enumerate(combinations, 1):
        prefix = f"[{i:>3}/{total}]"
        cache_id, cached_data = get_cached(conn, term, location)

        if cached_data:
            logger.info("%s CACHED   '%s' in '%s'", prefix, term, location)
            stats["cached"] += 1
            data = cached_data
        else:
            if dry_run:
                logger.info("%s WOULD CALL '%s' in '%s'", prefix, term, location)
                continue
            logger.info("%s CALLING  '%s' in '%s'", prefix, term, location)
            try:
                data = search_maps(term, location)
                cache_id = save_cache(conn, term, location, "maps", data)
                stats["api_calls"] += 1
                logger.info("         -> saved to cache id=%d", cache_id)
            except Exception as exc:
                logger.error("         -> API call failed: %s", exc)
                stats["errors"] += 1
                continue

        places = data.get("places", [])
        logger.info("         -> %d places found", len(places))

        for pos, place in enumerate(places, 1):
            try:
                r_id = upsert_restaurant(conn, place)
                if r_id and cache_id:
                    link_search_result(conn, cache_id, r_id, pos)
                    stats["restaurants"] += 1
            except Exception as exc:
                logger.error("         -> error saving '%s': %s", place.get("title"), exc)
                stats["errors"] += 1

    conn.close()

    logger.info("=" * 60)
    logger.info("Done.")
    logger.info("  API calls made : %d", stats["api_calls"])
    logger.info("  From cache     : %d", stats["cached"])
    logger.info("  Restaurants    : %d", stats["restaurants"])
    logger.info("  Errors         : %d", stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    run(dry_run="--dry-run" in sys.argv)

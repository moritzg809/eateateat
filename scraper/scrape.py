"""
mallorcaeat scraper — Step 1
Iterates over all (search_term × location) combinations, caches each
Serper Maps response, and upserts results into the restaurants table.

Usage:
    python scrape.py                    # run only due queries (> 6 months old)
    python scrape.py --force            # re-run all queries regardless of age
    python scrape.py --dry-run          # show what would be called / skipped
    python scrape.py --init             # seed pipeline_runs from config, then exit
"""

import logging
import os
import sys
import time

import requests

from config import LOCATIONS, SEARCH_TERMS
from db import (
    get_connection,
    get_cached,
    get_due_pipeline_runs,
    init_pipeline_runs,
    link_search_result,
    mark_pipeline_run,
    save_cache,
    set_pipeline_status,
    upsert_restaurant,
)
from serper import search_maps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PHOTOS_DIR = os.getenv("PHOTOS_DIR", "/photos")
_SESSION = requests.Session()


def _download_thumbnail(place_id: str, url: str) -> None:
    """Download the Serper thumbnail as 0.jpg fallback. Skips if already exists."""
    if not url:
        return
    path = os.path.join(PHOTOS_DIR, place_id, "0.jpg")
    if os.path.exists(path):
        return
    try:
        resp = _SESSION.get(url, timeout=15)
        resp.raise_for_status()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(resp.content)
        time.sleep(0.05)
    except Exception as exc:
        logger.debug("Thumbnail download failed for %s: %s", place_id, exc)


def run(dry_run: bool = False, force: bool = False, init_only: bool = False,
        search_terms: list | None = None, locations: list | None = None,
        city_id: int = 1) -> None:
    """Run the search stage. Accepts city-specific overrides from pipeline.py."""
    _terms     = search_terms or SEARCH_TERMS
    _locations = locations    or LOCATIONS

    conn = get_connection()

    # Always ensure pipeline_runs table is seeded with current config
    init_pipeline_runs(conn, _terms, _locations)
    if init_only:
        logger.info("pipeline_runs initialised — exiting (--init mode)")
        conn.close()
        return

    # Determine which queries to run
    if force:
        combinations = [(term, loc) for loc in _locations for term in _terms]
        logger.info("FORCE mode — running all %d combinations", len(combinations))
    else:
        due_rows = get_due_pipeline_runs(conn)
        combinations = [(row[0], row[1]) for row in due_rows]
        if not combinations:
            logger.info("All search queries are fresh (< 6 months old). Nothing to do.")
            logger.info("Use --force to re-run anyway.")
            conn.close()
            return

    total = len(combinations)

    logger.info("=" * 60)
    logger.info("EatEatEat scraper — Step 1 (city_id=%d)", city_id)
    logger.info("  combinations due : %d", total)
    if dry_run:
        logger.info("  MODE             : DRY RUN (no API calls)")
    if force:
        logger.info("  MODE             : FORCE (bypass 6-month TTL)")
    logger.info("=" * 60)

    stats = {"api_calls": 0, "cached": 0, "restaurants": 0, "errors": 0}

    for i, (term, location) in enumerate(combinations, 1):
        prefix = f"[{i:>3}/{total}]"

        # Force-refresh: bypass cache if force=True
        cache_id, cached_data = get_cached(conn, term, location)
        if cached_data and not force:
            logger.info("%s CACHED   '%s' in '%s'", prefix, term, location)
            stats["cached"] += 1
            data = cached_data
        else:
            if dry_run:
                logger.info("%s WOULD CALL '%s' in '%s'", prefix, term, location)
                continue
            action = "FORCE-REFRESH" if (cached_data and force) else "CALLING "
            logger.info("%s %s  '%s' in '%s'", prefix, action, term, location)
            try:
                data = search_maps(term, location)
                cache_id = save_cache(conn, term, location, "maps", data)
                stats["api_calls"] += 1
                logger.info("         -> saved to cache id=%d", cache_id)
            except Exception as exc:
                logger.error("         -> API call failed: %s", exc)
                stats["errors"] += 1
                mark_pipeline_run(conn, term, location, 0, "error")
                continue

        places = data.get("places", [])
        logger.info("         -> %d places found", len(places))

        new_count = 0
        for pos, place in enumerate(places, 1):
            try:
                r_id = upsert_restaurant(conn, place, city_id=city_id)
                if r_id and cache_id:
                    link_search_result(conn, cache_id, r_id, pos)
                    # Set pipeline_status to 'new' only if not already in pipeline
                    # (won't downgrade a 'complete' restaurant)
                    place_id = place.get("placeId") or place.get("cid")
                    if place_id:
                        set_pipeline_status(conn, place_id, "new")
                        # Download Serper thumbnail as fallback photo (0.jpg)
                        _download_thumbnail(place_id, place.get("thumbnailUrl"))
                    stats["restaurants"] += 1
                    new_count += 1
            except Exception as exc:
                logger.error("         -> error saving '%s': %s", place.get("title"), exc)
                stats["errors"] += 1

        mark_pipeline_run(conn, term, location, new_count)

    conn.close()

    logger.info("=" * 60)
    logger.info("Done.")
    logger.info("  API calls made : %d", stats["api_calls"])
    logger.info("  From cache     : %d", stats["cached"])
    logger.info("  Restaurants    : %d", stats["restaurants"])
    logger.info("  Errors         : %d", stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    run(
        dry_run="--dry-run" in sys.argv,
        force="--force" in sys.argv,
        init_only="--init" in sys.argv,
    )

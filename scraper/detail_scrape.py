"""
mallorcaeat detail scraper — Step 1.5

Calls SerpAPI Google Maps Place Details for each top restaurant.
Fetches structured "About" data: highlights, popular_for, offerings,
atmosphere, crowd, planning, payments, accessibility, children, parking,
service_options.

Results are cached by Google Place ID — each restaurant is fetched at most once.

Usage:
    python detail_scrape.py               # fetch all unfetched top restaurants
    python detail_scrape.py --limit 10    # test with 10 restaurants
    python detail_scrape.py --dry-run     # show what would be called, no API costs
    python detail_scrape.py --force       # re-fetch even if already cached
"""

import argparse
import json
import logging
import os
import time

import requests

from db import get_connection, set_pipeline_status_force
from keys import KeyRotator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SERPAPI_URL = "https://serpapi.com/search"
PHOTOS_DIR  = os.getenv("PHOTOS_DIR", "/photos")

# Photo download settings
_SKIP_PHOTO_TITLES = {"all", "latest", "videos", "street view & 360°",
                      "street view", "360°", "popular dishes"}
_PHOTO_ORDER = ["by owner", "exterior", "food & drink",
                "from visitors", "amenities", "atmosphere", "rooms"]
MAX_PHOTOS = 8

_SESSION = requests.Session()
_rotator: KeyRotator | None = None


def _get_rotator() -> KeyRotator:
    global _rotator
    if _rotator is None:
        _rotator = KeyRotator.from_env("SERPAPI_API_KEYS", "SERPAPI_API_KEY")
    return _rotator


# ---------------------------------------------------------------------------
# Photo caching
# ---------------------------------------------------------------------------

def _photo_sort_key(cat: str) -> tuple:
    try:
        return (0, _PHOTO_ORDER.index(cat))
    except ValueError:
        return (1, cat)


def download_photos(place_id: str, images: list) -> int:
    """
    Download and cache restaurant photos to PHOTOS_DIR/{place_id}/.
    Files are saved as 0.jpg, 1.jpg, … in priority order (by owner first).
    Already-cached files are skipped. Returns count of photos now on disk.
    """
    dest_dir = os.path.join(PHOTOS_DIR, place_id)

    # Build sorted candidate list: (category, url)
    candidates: list[tuple[str, str]] = []
    for img in images:
        title = (img.get("title") or "").lower()
        if title in _SKIP_PHOTO_TITLES:
            continue
        # serpapi_thumbnail is SerpAPI-hosted — no hotlink issues
        url = img.get("serpapi_thumbnail") or img.get("thumbnail", "")
        if url:
            candidates.append((title, url))

    candidates.sort(key=lambda x: _photo_sort_key(x[0]))
    candidates = candidates[:MAX_PHOTOS]

    if not candidates:
        return 0

    os.makedirs(dest_dir, exist_ok=True)
    count = 0

    for idx, (category, url) in enumerate(candidates):
        path = os.path.join(dest_dir, f"{idx}.jpg")
        if os.path.exists(path):
            count += 1
            continue
        try:
            resp = _SESSION.get(url, timeout=15)
            resp.raise_for_status()
            with open(path, "wb") as fh:
                fh.write(resp.content)
            count += 1
            time.sleep(0.05)
        except Exception as exc:
            logger.warning("Photo %d (%s) download failed: %s", idx, category, exc)

    return count


# ---------------------------------------------------------------------------
# SerpAPI call
# ---------------------------------------------------------------------------

def _parse_extensions(extensions: list) -> dict:
    """
    Flatten the extensions array (list of single-key dicts) into a plain dict.
    Example: [{"highlights": [...]}, {"popular_for": [...]}]
          -> {"highlights": [...], "popular_for": [...]}
    """
    result = {}
    for ext in extensions:
        result.update(ext)
    return result


def fetch_place_details(data_cid: str, retries: int = 5) -> tuple[dict, dict]:
    """
    Call SerpAPI Google Maps Place Details via data_cid (decimal CID).
    Returns (place_results dict, full raw api response) or raises on failure.
    429 rate-limit responses rotate keys first, then use longer backoff.
    """
    rotator = _get_rotator()

    for attempt in range(1, retries + 1):
        params = {
            "engine":   "google_maps",
            "type":     "place",
            "data_cid": data_cid,
            "hl":       "en",          # English for consistent field names
            "api_key":  rotator.current(),
        }
        try:
            resp = _SESSION.get(SERPAPI_URL, params=params, timeout=30)
            resp.raise_for_status()
            raw = resp.json()
            if "error" in raw:
                raise ValueError(f"SerpAPI error: {raw['error']}")
            rotator.reset()
            return raw.get("place_results", {}), raw
        except requests.HTTPError:
            if resp.status_code == 429:
                if rotator.rotate():
                    logger.warning("429 from SerpAPI – rotated key (attempt %d/%d)",
                                   attempt, retries)
                    continue
                else:
                    wait = 30
                    logger.warning("429 SerpAPI – all keys exhausted, waiting %ss", wait)
                    time.sleep(wait)
                    rotator.reset()
                    continue
            if attempt == retries:
                raise
            wait = 2 ** attempt
            logger.warning("HTTP %s – retry in %ss (%d/%d)",
                           resp.status_code, wait, attempt, retries)
            time.sleep(wait)
        except Exception as e:
            if attempt == retries:
                raise
            logger.warning("Error attempt %d/%d: %s", attempt, retries, e)
            time.sleep(2 ** attempt)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def fetch_pending(conn, min_rating: float, min_reviews: int,
                  limit: int | None, force: bool) -> list:
    if force:
        join = ""
        where_cache = ""
    else:
        join = "LEFT JOIN serpapi_details sd ON sd.place_id = r.place_id"
        where_cache = "AND sd.place_id IS NULL"

    sql = f"""
        SELECT r.id, r.place_id, r.name, r.raw_data->>'cid' AS data_cid
        FROM   restaurants r
        {join}
        WHERE  r.rating       >= %(min_rating)s
          AND  r.rating_count >= %(min_reviews)s
          AND  r.raw_data->>'cid' IS NOT NULL
          {where_cache}
        ORDER BY r.rating DESC, r.rating_count DESC
        {"LIMIT %(limit)s" if limit else ""}
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"min_rating": min_rating,
                          "min_reviews": min_reviews,
                          "limit": limit})
        return cur.fetchall()


def is_place_closed(place: dict) -> bool:
    """Return True if SerpAPI data indicates the place is permanently or temporarily closed."""
    return bool(
        place.get("permanently_closed")
        or place.get("temporarily_closed")
        or place.get("closed_on_permanently")  # alternate field name
    )


def save_details(conn, place_id: str, place: dict, raw_response: dict | None = None):
    extensions = _parse_extensions(place.get("extensions", []))
    service_options = place.get("service_options", {})

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO serpapi_details (
                place_id,
                highlights, popular_for, offerings, atmosphere,
                crowd, planning, payments, accessibility,
                children, parking, dining_options, amenities,
                service_options, raw_extensions, raw_response
            ) VALUES (
                %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (place_id) DO UPDATE SET
                highlights      = EXCLUDED.highlights,
                popular_for     = EXCLUDED.popular_for,
                offerings       = EXCLUDED.offerings,
                atmosphere      = EXCLUDED.atmosphere,
                crowd           = EXCLUDED.crowd,
                planning        = EXCLUDED.planning,
                payments        = EXCLUDED.payments,
                accessibility   = EXCLUDED.accessibility,
                children        = EXCLUDED.children,
                parking         = EXCLUDED.parking,
                dining_options  = EXCLUDED.dining_options,
                amenities       = EXCLUDED.amenities,
                service_options = EXCLUDED.service_options,
                raw_extensions  = EXCLUDED.raw_extensions,
                raw_response    = EXCLUDED.raw_response,
                fetched_at      = NOW()
            """,
            (
                place_id,
                extensions.get("highlights"),
                extensions.get("popular_for"),
                extensions.get("offerings"),
                extensions.get("atmosphere"),
                extensions.get("crowd"),
                extensions.get("planning"),
                extensions.get("payments"),
                extensions.get("accessibility"),
                extensions.get("children"),
                extensions.get("parking"),
                extensions.get("dining_options"),
                extensions.get("amenities"),
                json.dumps(service_options) if service_options else None,
                json.dumps(extensions) if extensions else None,
                json.dumps(raw_response) if raw_response else None,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(limit=None, min_rating=4.5, min_reviews=100, dry_run=False, force=False):
    conn = get_connection()
    rows = fetch_pending(conn, min_rating, min_reviews, limit, force)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat detail scraper — SerpAPI Place Details")
    logger.info("  pending     : %d restaurants", total)
    logger.info("  min rating  : %.1f   min reviews: %d", min_rating, min_reviews)
    logger.info("  cache key   : place_id (pay once per restaurant)")
    if dry_run:
        logger.info("  MODE        : DRY RUN (no API calls, no costs)")
    if force:
        logger.info("  MODE        : FORCE (re-fetching cached entries)")
    logger.info("=" * 60)

    stats = {"ok": 0, "empty": 0, "errors": 0}

    for i, (rid, place_id, name, data_cid) in enumerate(rows, 1):
        prefix = f"[{i:>3}/{total}]"

        if dry_run:
            logger.info("%s WOULD FETCH  %s", prefix, name)
            continue

        logger.info("%s %s", prefix, name)
        try:
            place, raw_response = fetch_place_details(data_cid)
            save_details(conn, place_id, place, raw_response)

            # Download and cache photos locally
            images = raw_response.get("place_results", {}).get("images", [])
            if images:
                n_photos = download_photos(place_id, images)
                if n_photos:
                    logger.info("         -> 📷 %d photos cached", n_photos)

            # Closed restaurant detection
            if is_place_closed(place):
                set_pipeline_status_force(conn, place_id, "inactive")
                logger.info("         -> 🚫 CLOSED — marked inactive")
                stats["closed"] = stats.get("closed", 0) + 1
                time.sleep(0.5)
                continue

            ext = _parse_extensions(place.get("extensions", []))
            highlights = ext.get("highlights", [])
            popular_for = ext.get("popular_for", [])
            offerings = ext.get("offerings", [])

            if highlights or popular_for or offerings:
                stats["ok"] += 1
                preview = " | ".join(filter(None, [
                    f"highlights: {highlights[:2]}",
                    f"popular_for: {popular_for[:2]}",
                ]))
                logger.info("         -> ✓  %s", preview[:100])
            else:
                stats["empty"] += 1
                logger.info("         -> ○  (no extensions data)")

            time.sleep(0.5)  # gentle pacing
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    conn.close()
    logger.info("=" * 60)
    logger.info("Done.  OK: %d | Empty: %d | Closed: %d | Errors: %d",
                stats["ok"], stats["empty"], stats.get("closed", 0), stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Fetch SerpAPI Place Details for top restaurants")
    ap.add_argument("--limit",       type=int,   default=None,
                    help="max restaurants to process")
    ap.add_argument("--min-rating",  type=float, default=4.5,
                    help="minimum Google rating")
    ap.add_argument("--min-reviews", type=int,   default=100,
                    help="minimum review count")
    ap.add_argument("--dry-run",     action="store_true",
                    help="no API calls, just show what would run")
    ap.add_argument("--force",       action="store_true",
                    help="re-fetch even if place_id already cached")
    args = ap.parse_args()

    run(
        limit=args.limit,
        min_rating=args.min_rating,
        min_reviews=args.min_reviews,
        dry_run=args.dry_run,
        force=args.force,
    )

"""
mallorcaeat — backfill photo downloads from stored raw_response

Pass 1: reads image URLs from serpapi_details.raw_response and downloads
         them as .jpg files into PHOTOS_DIR/{place_id}/.
Pass 2: for restaurants still without photos, downloads the Serper
         thumbnail_url from top_restaurants as 0.jpg (fallback).
No extra API calls or credit spending.

Usage:
    python backfill_photos.py            # download all missing photos
    python backfill_photos.py --dry-run  # show what would be downloaded
"""

import argparse
import logging
import os
import time

import requests

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PHOTOS_DIR = os.getenv("PHOTOS_DIR", "/photos")
MAX_PHOTOS = 8

_SKIP_PHOTO_TITLES = {
    "all", "latest", "videos", "street view & 360°",
    "360°", "popular dishes", "menu",
}
_PHOTO_ORDER = [
    "by owner", "exterior", "food & drink", "vibe",
    "from visitors", "inside", "street view",
    "amenities", "atmosphere", "rooms",
]

_SESSION = requests.Session()


def _photo_sort_key(cat: str) -> tuple:
    try:
        return (0, _PHOTO_ORDER.index(cat))
    except ValueError:
        return (1, cat)


def download_photos(place_id: str, images: list, dry_run: bool = False) -> int:
    """Download images to PHOTOS_DIR/{place_id}/. Skips already-existing files."""
    dest_dir = os.path.join(PHOTOS_DIR, place_id)

    candidates: list[tuple[str, str]] = []
    for img in images:
        title = (img.get("title") or "").lower()
        if title in _SKIP_PHOTO_TITLES:
            continue
        url = img.get("serpapi_thumbnail") or img.get("thumbnail", "")
        if url:
            candidates.append((title, url))

    candidates.sort(key=lambda x: _photo_sort_key(x[0]))
    candidates = candidates[:MAX_PHOTOS]

    if not candidates:
        return 0

    if dry_run:
        logger.info("  DRY-RUN: would download %d photos to %s", len(candidates), dest_dir)
        return len(candidates)

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
            logger.warning("  Photo %d (%s) failed: %s", idx, category, exc)

    return count


def main():
    parser = argparse.ArgumentParser(description="Backfill photos from stored raw_response")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done, no downloads")
    args = parser.parse_args()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sd.place_id, r.name,
                       sd.raw_response->'place_results'->'images' AS images
                FROM serpapi_details sd
                JOIN top_restaurants r ON r.place_id = sd.place_id
                WHERE sd.raw_response->'place_results'->'images' IS NOT NULL
                ORDER BY r.name
            """)
            rows = cur.fetchall()

    total = len(rows)
    logger.info("Found %d restaurants with images in DB", total)

    already_done = 0
    downloaded = 0
    skipped = 0

    for i, (place_id, name, images) in enumerate(rows, 1):
        # Check if photos already exist on disk
        dest_dir = os.path.join(PHOTOS_DIR, place_id)
        if os.path.isdir(dest_dir) and os.path.exists(os.path.join(dest_dir, "0.jpg")):
            already_done += 1
            continue

        logger.info("[%3d/%d] %s", i, total, name)
        n = download_photos(place_id, images, dry_run=args.dry_run)
        if n:
            downloaded += n
        else:
            skipped += 1

    logger.info("Pass 1 done — %d already on disk, %d photos downloaded, %d had no usable images",
                already_done, downloaded, skipped)

    # ── Pass 2: Serper thumbnail fallback ──────────────────────────────────────
    logger.info("Pass 2: downloading Serper thumbnails for restaurants without any photos...")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT place_id, name, thumbnail_url
                FROM top_restaurants
                WHERE thumbnail_url IS NOT NULL
                ORDER BY name
            """)
            all_rows = cur.fetchall()

    thumb_downloaded = 0
    thumb_skipped = 0

    for place_id, name, thumbnail_url in all_rows:
        dest_dir = os.path.join(PHOTOS_DIR, place_id)
        path = os.path.join(dest_dir, "0.jpg")
        if os.path.exists(path):
            continue  # already has photos from Pass 1 or prior run

        if args.dry_run:
            logger.info("  DRY-RUN: would download Serper thumbnail for %s", name)
            thumb_downloaded += 1
            continue

        try:
            resp = _SESSION.get(thumbnail_url, timeout=15)
            resp.raise_for_status()
            os.makedirs(dest_dir, exist_ok=True)
            with open(path, "wb") as fh:
                fh.write(resp.content)
            thumb_downloaded += 1
            time.sleep(0.05)
        except Exception as exc:
            logger.warning("  Serper thumbnail failed for %s: %s", name, exc)
            thumb_skipped += 1

    logger.info("Pass 2 done — %d Serper thumbnails downloaded, %d failed",
                thumb_downloaded, thumb_skipped)


if __name__ == "__main__":
    main()

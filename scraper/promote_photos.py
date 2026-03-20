"""
mallorcaeat — Promote classified website images into the photos directory

Reads CLIP classification results from /website_scrapes/{place_id}/meta.json
and copies all GOOD images to /photos/{place_id}/{n}_websiteScraper.jpg.

Idempotent: skips images already promoted (checks dest file existence).

Usage:
    python promote_photos.py               # promote all classified restaurants
    python promote_photos.py --dry-run     # preview only
    python promote_photos.py --force       # re-copy even if dest exists
"""

import argparse
import json
import logging
import os
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

WEBSITE_SCRAPES_DIR = os.getenv("WEBSITE_SCRAPES_DIR", "/website_scrapes")
PHOTOS_DIR          = os.getenv("PHOTOS_DIR",          "/photos")


def promote_restaurant(place_id: str, dry_run: bool = False, force: bool = False) -> dict:
    scrape_dir = os.path.join(WEBSITE_SCRAPES_DIR, place_id)
    meta_path  = os.path.join(scrape_dir, "meta.json")

    if not os.path.exists(meta_path):
        return {"status": "no_meta"}

    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)

    classifications = meta.get("image_classifications", {})
    if not classifications:
        return {"status": "no_classifications"}

    # Collect GOOD images in stable order
    good_files = sorted(
        filename
        for filename, info in classifications.items()
        if info.get("label") == "GOOD"
    )

    if not good_files:
        return {"status": "no_good_images"}

    dest_dir = os.path.join(PHOTOS_DIR, place_id)
    os.makedirs(dest_dir, exist_ok=True)

    promoted = 0
    skipped  = 0

    for n, filename in enumerate(good_files):
        src  = os.path.join(scrape_dir, filename)
        dest = os.path.join(dest_dir, f"{n}_websiteScraper.jpg")

        if not os.path.exists(src):
            logger.debug("  Source missing: %s", src)
            continue

        if not force and os.path.exists(dest):
            skipped += 1
            continue

        if dry_run:
            logger.info("  DRY-RUN: %s → %s", filename, dest)
            promoted += 1
            continue

        shutil.copy2(src, dest)
        promoted += 1

    return {"status": "ok", "promoted": promoted, "skipped": skipped}


def run(dry_run: bool = False, force: bool = False):
    """Promote GOOD website images for all scraped restaurants."""
    if not os.path.isdir(WEBSITE_SCRAPES_DIR):
        logger.warning("WEBSITE_SCRAPES_DIR not found: %s", WEBSITE_SCRAPES_DIR)
        return

    place_ids = [
        d for d in os.listdir(WEBSITE_SCRAPES_DIR)
        if not d.startswith("_") and os.path.isdir(os.path.join(WEBSITE_SCRAPES_DIR, d))
    ]

    total      = len(place_ids)
    promoted   = 0
    no_good    = 0
    errors     = 0

    logger.info("Found %d scraped restaurants to process", total)

    for i, place_id in enumerate(sorted(place_ids), 1):
        try:
            result = promote_restaurant(place_id, dry_run=dry_run, force=force)
            status = result.get("status")

            if status == "ok":
                n = result["promoted"]
                s = result["skipped"]
                if n > 0:
                    logger.info("[%4d/%d] %s → promoted %d image(s) (%d already existed)",
                                i, total, place_id, n, s)
                promoted += n
            elif status in ("no_meta", "no_classifications", "no_good_images"):
                no_good += 1
        except Exception as exc:
            logger.warning("[%4d/%d] %s ERROR: %s", i, total, place_id, exc)
            errors += 1

    logger.info(
        "Done — %d image(s) promoted, %d restaurants with no GOOD images, %d errors",
        promoted, no_good, errors,
    )


def main():
    ap = argparse.ArgumentParser(description="Promote GOOD website images to photos directory")
    ap.add_argument("--dry-run", action="store_true", help="Preview without copying")
    ap.add_argument("--force",   action="store_true", help="Re-copy even if dest exists")
    args = ap.parse_args()
    run(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()

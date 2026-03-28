"""
mallorcaeat — CLIP-based Image Classifier

Classifies scraped website images as GOOD / UNSURE / BAD using CLIP zero-shot
similarity. Results are stored in the restaurant's meta.json so the frontend
can filter accordingly.

Labels:
  GOOD   — food, drinks, restaurant interior/ambiance → show on website
  UNSURE — borderline cases → hide by default, can be reviewed later
  BAD    — logos, maps, icons, text-only, screenshots → never show

Usage:
    python image_classifier.py              # classify all unclassified images
    python image_classifier.py --force      # re-classify even if already done
    python image_classifier.py --limit 50   # process at most 50 restaurants
    python image_classifier.py --show-scores # print scores per image (debug)
"""

import argparse
import glob
import json
import logging
import os

from PIL import Image
from sentence_transformers import SentenceTransformer
from sentence_transformers.util import cos_sim

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

WEBSITE_SCRAPES_DIR = os.getenv("WEBSITE_SCRAPES_DIR", "/website_scrapes")

# ── CLIP prompts ──────────────────────────────────────────────────────────────
#
# Positive = what we WANT to show (restaurant guide imagery)
# Negative = what we DON'T want (logos, UI elements, boring stuff)
#
# Tip: more specific prompts → better discrimination

_POSITIVE_PROMPTS = [
    "food on a plate",
    "restaurant meal",
    "dish served at a restaurant",
    "delicious food photography",
    "restaurant interior with tables",
    "dining room ambiance",
    "restaurant terrace outdoor seating",
    "cocktail drink at a bar",
    "wine glass at a restaurant",
    "fresh ingredients food",
    "dessert on a plate",
    "restaurant kitchen",
]

_NEGATIVE_PROMPTS = [
    "company logo",
    "map or floor plan",
    "icon or symbol",
    "website screenshot",
    "text document",
    "QR code",
    "business card",
    "parking sign",
    "promotional banner with text",
    "person portrait headshot",
    "staff photo",
    "blank white image",
]

# Thresholds: score = avg(pos_sims) - avg(neg_sims)
# Calibrated via 100-image human annotation (2026-03-28):
#   score ≥ -0.02 → nearly always GOOD in human review
#   score <  -0.02 → bad/unsure
_GOOD_THRESHOLD  = -0.02   # score > this → GOOD
_BAD_THRESHOLD   = -0.05   # score < this → BAD
                            # in between   → UNSURE


# ── Model (loaded once) ───────────────────────────────────────────────────────

_model: SentenceTransformer | None = None

def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        logger.info("Loading CLIP model (clip-ViT-L-14)…")
        _model = SentenceTransformer("clip-ViT-L-14")
        logger.info("Model ready.")
    return _model


# ── Classification ────────────────────────────────────────────────────────────

def _encode_prompts(model: SentenceTransformer) -> tuple:
    pos_embs = model.encode(_POSITIVE_PROMPTS, convert_to_tensor=True)
    neg_embs = model.encode(_NEGATIVE_PROMPTS, convert_to_tensor=True)
    return pos_embs, neg_embs


def classify_image(path: str, model: SentenceTransformer,
                   pos_embs, neg_embs) -> dict:
    """Return {"label": str, "score": float, "pos": float, "neg": float}."""
    try:
        img = Image.open(path).convert("RGB")
        img_emb = model.encode(img, convert_to_tensor=True)

        pos_sims = cos_sim(img_emb, pos_embs)[0]
        neg_sims = cos_sim(img_emb, neg_embs)[0]

        pos_score = float(pos_sims.mean())
        neg_score = float(neg_sims.mean())
        score     = pos_score - neg_score

        if score > _GOOD_THRESHOLD:
            label = "GOOD"
        elif score < _BAD_THRESHOLD:
            label = "BAD"
        else:
            label = "UNSURE"

        return {"label": label, "score": round(score, 4),
                "pos": round(pos_score, 4), "neg": round(neg_score, 4)}

    except Exception as exc:
        logger.warning("  Could not classify %s: %s", os.path.basename(path), exc)
        return {"label": "UNSURE", "score": 0.0, "pos": 0.0, "neg": 0.0}


# ── Process one restaurant ────────────────────────────────────────────────────

def classify_restaurant(place_id: str, model: SentenceTransformer,
                        pos_embs, neg_embs,
                        force: bool = False,
                        show_scores: bool = False) -> dict:
    folder    = os.path.join(WEBSITE_SCRAPES_DIR, place_id)
    meta_path = os.path.join(folder, "meta.json")

    if not os.path.exists(meta_path):
        return {"status": "no_meta"}

    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)

    existing = meta.get("image_classifications", {})

    images = sorted(glob.glob(os.path.join(folder, f"{place_id}-WebsiteScraper-*.jpg")))
    if not images:
        return {"status": "no_images"}

    classifications = dict(existing)
    classified = 0

    for path in images:
        fname = os.path.basename(path)
        if not force and fname in existing:
            continue
        result = classify_image(path, model, pos_embs, neg_embs)
        classifications[fname] = result
        classified += 1
        if show_scores:
            logger.info("    %s → %s (score=%.3f)", fname, result["label"], result["score"])

    meta["image_classifications"] = classifications
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    counts = {}
    for v in classifications.values():
        counts[v["label"]] = counts.get(v["label"], 0) + 1

    return {"status": "ok", "classified": classified, "counts": counts}


# ── Main / run ────────────────────────────────────────────────────────────────

def run(limit: int | None = None, force: bool = False, show_scores: bool = False):
    folders = [
        d for d in os.listdir(WEBSITE_SCRAPES_DIR)
        if os.path.isdir(os.path.join(WEBSITE_SCRAPES_DIR, d))
        and not d.startswith("_")
    ]
    folders.sort()
    if limit:
        folders = folders[:limit]

    total = len(folders)
    logger.info("Found %d restaurant folders to classify", total)

    model = _get_model()
    logger.info("Encoding %d positive + %d negative prompts…",
                len(_POSITIVE_PROMPTS), len(_NEGATIVE_PROMPTS))
    pos_embs, neg_embs = _encode_prompts(model)

    processed = skipped = errors = 0
    total_counts: dict[str, int] = {}

    for i, place_id in enumerate(folders, 1):
        prefix = f"[{i:>4}/{total}]"
        try:
            result = classify_restaurant(
                place_id, model, pos_embs, neg_embs,
                force=force, show_scores=show_scores,
            )
            if result["status"] == "no_meta":
                skipped += 1
            elif result["status"] == "no_images":
                skipped += 1
            else:
                classified = result["classified"]
                counts     = result["counts"]
                if classified > 0:
                    logger.info("%s %s → %s", prefix, place_id, counts)
                    for label, n in counts.items():
                        total_counts[label] = total_counts.get(label, 0) + n
                processed += 1
        except Exception as exc:
            logger.warning("%s ERROR: %s", prefix, exc)
            errors += 1

    logger.info(
        "Done — %d processed, %d skipped, %d errors",
        processed, skipped, errors,
    )
    logger.info("Overall image labels: %s", total_counts)


def main():
    ap = argparse.ArgumentParser(description="CLIP-based image classifier for scraped restaurant photos")
    ap.add_argument("--force",       action="store_true", help="Re-classify even if already done")
    ap.add_argument("--limit",       type=int,            help="Max restaurants to process")
    ap.add_argument("--show-scores", action="store_true", help="Print per-image scores")
    args = ap.parse_args()
    run(limit=args.limit, force=args.force, show_scores=args.show_scores)


if __name__ == "__main__":
    main()

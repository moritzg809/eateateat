"""
Compute and store curation_score for all restaurants in gemini_enrichments.

Score formula (0–100):
  critic      × 25  — Gemini editorial quality (1–10)
  quality     × 20  — avg(unique_score, foodie_score, local_score) (1–10)
  rating      × 30  — Bayesian-smoothed Google rating, normalised to [4.0–5.0]
  completeness× 15  — has_article(0.50) + has_must_order(0.30) + has_photos(0.20)
  audience    × 10  — gourmet→1.0 | local→0.8 | mixed/business→0.5 | tourist→0.2

Usage:
  python compute_curation_score.py             # compute + write to DB
  python compute_curation_score.py --dry-run   # show top-20, no DB writes
"""
import argparse
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

PHOTOS_DIR = os.environ.get("PHOTOS_DIR", "/app/static/photos")  # Docker path
LOCAL_PHOTOS_DIR = os.path.join(
    os.path.dirname(__file__),
    "../frontend/static/photos"
)

BAYESIAN_K = 50          # prior review count
AUDIENCE_MAP = {
    "gourmet":  1.0,
    "local":    0.8,
    "business": 0.5,
    "mixed":    0.5,
    "family":   0.5,
    "tourist":  0.2,
}


def get_connection():
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://mallorcaeat:mallorcaeat_dev@localhost:5433/mallorcaeat"
    )
    return psycopg2.connect(url)


def has_photos(place_id: str) -> bool:
    for photos_dir in [LOCAL_PHOTOS_DIR, PHOTOS_DIR]:
        photo_path = os.path.join(photos_dir, place_id, "0.jpg")
        if os.path.exists(photo_path):
            return True
    return False


def compute_score(row: dict, global_mean_rating: float) -> float:
    # ── 1. Critic (25%) ───────────────────────────────────────────────────────
    critic_raw = row["critic_score"] or 5.0
    critic_component = (critic_raw / 10.0) * 25.0

    # ── 2. Quality (20%) ──────────────────────────────────────────────────────
    unique  = row["unique_score"]  or 5.0
    foodie  = row["foodie_score"]  or 5.0
    local   = row["local_score"]   or 5.0
    quality_component = ((unique + foodie + local) / 30.0) * 20.0

    # ── 3. Bayesian Rating (30%) ──────────────────────────────────────────────
    rating       = float(row["rating"] or global_mean_rating)
    rating_count = int(row["rating_count"] or 0)
    bayes_rating = (BAYESIAN_K * global_mean_rating + rating_count * rating) / (BAYESIAN_K + rating_count)
    # Normalise: 4.0 → 0.0, 5.0 → 1.0 (clamp)
    rating_norm = max(0.0, min(1.0, (bayes_rating - 4.0) / 1.0))
    rating_component = rating_norm * 30.0

    # ── 4. Completeness (15%) ─────────────────────────────────────────────────
    completeness = (
        (0.50 if row["has_article"] else 0.0)
        + (0.30 if row["must_order"] else 0.0)
        + (0.20 if has_photos(row["place_id"]) else 0.0)
    )
    completeness_component = completeness * 15.0

    # ── 5. Audience (10%) ─────────────────────────────────────────────────────
    audience_val = AUDIENCE_MAP.get(row["audience_type"] or "", 0.5)
    audience_component = audience_val * 10.0

    total = (
        critic_component
        + quality_component
        + rating_component
        + completeness_component
        + audience_component
    )
    return round(total, 2)


def run(dry_run: bool = False):
    """Entry point for pipeline.py integration."""
    main(dry_run=dry_run)


def main(dry_run: bool = False):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Global mean rating for Bayesian prior
            cur.execute("""
                SELECT AVG(rating)::float AS mean
                FROM restaurants
                WHERE rating IS NOT NULL AND rating_count > 50
            """)
            global_mean_rating = cur.fetchone()["mean"] or 4.5
            print(f"Global mean rating (Bayesian prior): {global_mean_rating:.3f}")

            # All enriched restaurants
            cur.execute("""
                SELECT
                    r.place_id,
                    r.rating,
                    r.rating_count,
                    e.critic_score,
                    e.unique_score,
                    e.foodie_score,
                    e.local_score,
                    e.audience_type,
                    e.must_order,
                    (ea.place_id IS NOT NULL) AS has_article
                FROM restaurants r
                JOIN gemini_enrichments e ON e.place_id = r.place_id
                LEFT JOIN editorial_articles ea
                    ON ea.place_id = r.place_id AND ea.is_published = TRUE
                WHERE r.pipeline_status = 'complete'
                  AND r.is_active = TRUE
            """)
            rows = cur.fetchall()

        print(f"Computing scores for {len(rows)} restaurants...")
        scored = []
        for row in rows:
            row = dict(row)
            score = compute_score(row, global_mean_rating)
            scored.append((score, row["place_id"], row))

        scored.sort(reverse=True, key=lambda x: x[0])

        # Show top 20
        print(f"\n{'Rank':<5} {'Score':<7} {'Article':<8} {'Audience':<12} {'Name / Place ID'}")
        print("─" * 80)
        for i, (score, pid, row) in enumerate(scored[:20], 1):
            article_flag = "★" if row["has_article"] else " "
            audience = (row["audience_type"] or "–")[:10]
            print(f"{i:<5} {score:<7.2f} {article_flag:<8} {audience:<12} {pid}")

        if dry_run:
            print("\n[dry-run] No changes written to DB.")
            return

        # Write to DB
        with conn.cursor() as cur:
            for score, pid, _ in scored:
                cur.execute(
                    "UPDATE gemini_enrichments SET curation_score = %s WHERE place_id = %s",
                    (score, pid)
                )
        conn.commit()
        print(f"\n✓ Updated curation_score for {len(scored)} restaurants.")

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute curation scores for all restaurants.")
    parser.add_argument("--dry-run", action="store_true", help="Show top-20 without writing to DB")
    args = parser.parse_args()
    main(dry_run=args.dry_run)

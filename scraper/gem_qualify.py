"""
mallorcaeat gem qualifier — Step 2.5

Calls Gemini for restaurants that are 'disqualified' (rating < 4.5 OR reviews < 100)
but have rating >= 4.0. Asks only for unique_score and foodie_score, passing the
rating and review count into the prompt so the model can self-moderate on data quality.

If either score > 8 → marks as qualified → appears in admin review queue.
Admin manually approves (sets pipeline_status = 'new') or rejects (rejected = TRUE).

Shares the 500/day Gemini quota with enrich.py — only runs when no primary
candidates (rating >= 4.5, reviews >= 100) are pending enrichment.

Usage:
    python gem_qualify.py               # qualify all pending restaurants
    python gem_qualify.py --limit 10    # test with 10
    python gem_qualify.py --dry-run     # no API calls
    python gem_qualify.py --min-rating 4.2  # stricter floor
"""

import argparse
import json
import logging
import os
import re
import time

from google import genai
from google.genai import types

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL              = "gemini-2.5-flash"
QUALIFY_THRESHOLD  = 8   # either score must exceed this to qualify for review

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

PREQUALIFY_PROMPT = """\
Du bewertest ob ein Restaurant in Mallorca trotz niedrigerer Bewertung oder \
weniger Reviews besonders interessant sein könnte.

Restaurant: "{name}" ({address})
Google-Bewertung: {rating:.1f}★ ({review_count} Bewertungen)
{context_note}
{website_section}\
Schlage "{name}" auf Google Maps nach und lies die echten Reviews.

WICHTIG ZUR DATENQUALITÄT:
Wenn du dir aufgrund weniger Reviews oder einer dünnen/fehlenden Website nicht \
sicher bist — gib niedrigere Scores zurück (6 oder weniger).
Lass dich nur dann zu Scores über 8 hinreißen, wenn die Website substanziell \
ist UND die Reviews authentisch wirken UND du wirklich überzeugt bist.

Bewerte NUR diese zwei Kriterien (1–10, ganze Zahlen):

• Geheimtipp (unique): Echter Geheimtipp, den Locals kennen aber Touristen nicht?
  1 = Touristenlokal / Kette, 10 = fast nur Einheimische, kein Reiseführer

• Foodie: Interessant für jemanden der wirklich gutes Essen sucht?
  1 = Tiefkühlware aufgewärmt, 10 = Küchenchef mit Handschrift, saisonale Küche

Antworte AUSSCHLIESSLICH mit diesem JSON (kein Markdown, kein Text davor/danach):
{{"unique": <int>, "foodie": <int>}}"""


def _context_note(rating: float, review_count: int) -> str:
    parts = []
    if rating < 4.5:
        parts.append(f"nur {rating:.1f}★")
    if review_count < 100:
        parts.append(f"nur {review_count} Bewertungen")
    if parts:
        return f"Hinweis zur Datenlage: dieses Restaurant hat {' und '.join(parts)}. Sei entsprechend vorsichtig."
    return ""


def call_gemini(
    name: str, address: str, rating: float, review_count: int,
    lat=None, lng=None, website: str | None = None, retries: int = 3,
) -> dict:
    website_section = f"Lies zuerst die offizielle Website: {website}\n" if website else ""
    prompt = PREQUALIFY_PROMPT.format(
        name=name,
        address=address or "Mallorca, Spanien",
        rating=float(rating or 0),
        review_count=int(review_count or 0),
        context_note=_context_note(float(rating or 0), int(review_count or 0)),
        website_section=website_section,
    )

    tools = [types.Tool(google_maps=types.GoogleMaps())]
    if website:
        tools.append(types.Tool(url_context=types.UrlContext()))

    config_kwargs: dict = {"tools": tools, "temperature": 0.2}
    if lat is not None and lng is not None:
        config_kwargs["tool_config"] = types.ToolConfig(
            retrieval_config=types.RetrievalConfig(
                lat_lng=types.LatLng(latitude=float(lat), longitude=float(lng))
            )
        )

    for attempt in range(1, retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs),
            )

            text = None
            try:
                text = response.text
            except Exception:
                for candidate in (response.candidates or []):
                    for part in (candidate.content.parts or []):
                        if hasattr(part, "text") and part.text:
                            text = (text or "") + part.text

            if not text or text.strip().lower() in ("none", "null", ""):
                raise ValueError(f"Empty response: {text!r}")

            text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                text = match.group(0)

            parsed = json.loads(text)
            return {
                "unique": max(1, min(10, int(parsed["unique"]))),
                "foodie": max(1, min(10, int(parsed["foodie"]))),
            }

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Parse error attempt %d/%d: %s", attempt, retries, e)
            if attempt == retries:
                raise
            time.sleep(2)
        except Exception as exc:
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning("API error – retry in %ds (%d/%d): %s", wait, attempt, retries, exc)
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gemini_prequalify (
                place_id     TEXT PRIMARY KEY,
                unique_score SMALLINT CHECK (unique_score BETWEEN 1 AND 10),
                foodie_score SMALLINT CHECK (foodie_score BETWEEN 1 AND 10),
                qualified    BOOLEAN  NOT NULL DEFAULT FALSE,
                rejected     BOOLEAN  NOT NULL DEFAULT FALSE,
                evaluated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_prequalify_candidates
                ON gemini_prequalify (qualified, rejected)
                WHERE qualified = TRUE AND rejected = FALSE
        """)
    conn.commit()


def fetch_pending(conn, min_rating: float = 4.0, limit: int | None = None) -> list:
    """Disqualified restaurants with rating >= min_rating not yet evaluated."""
    limit_cl = "LIMIT %(limit)s" if limit else ""
    sql = f"""
        SELECT r.id, r.place_id, r.name, r.address,
               r.rating, r.rating_count, r.latitude, r.longitude, r.website
        FROM   restaurants r
        LEFT JOIN gemini_prequalify p ON p.place_id = r.place_id
        WHERE  r.pipeline_status = 'disqualified'
          AND  r.rating >= %(min_rating)s
          AND  p.place_id IS NULL
        ORDER  BY r.rating DESC, r.rating_count DESC
        {limit_cl}
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"min_rating": min_rating, "limit": limit})
        return cur.fetchall()


def save_prequalify(conn, place_id: str, unique_score: int, foodie_score: int) -> bool:
    """Save scores; returns True if restaurant qualifies for review."""
    qualified = unique_score > QUALIFY_THRESHOLD or foodie_score > QUALIFY_THRESHOLD
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gemini_prequalify (place_id, unique_score, foodie_score, qualified)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (place_id) DO UPDATE SET
                unique_score = EXCLUDED.unique_score,
                foodie_score = EXCLUDED.foodie_score,
                qualified    = EXCLUDED.qualified,
                evaluated_at = NOW()
            """,
            (place_id, unique_score, foodie_score, qualified),
        )
    conn.commit()
    return qualified


def count_today_prequalify(conn) -> int:
    """Count gem_qualify Gemini calls made today (for shared daily cap)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM gemini_prequalify WHERE evaluated_at::date = CURRENT_DATE"
            )
            return cur.fetchone()[0]
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(limit=None, min_rating=4.0, dry_run=False):
    conn = get_connection()
    ensure_schema(conn)

    rows  = fetch_pending(conn, min_rating=min_rating, limit=limit)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat gem qualifier — Gemini pre-qualification")
    logger.info("  model      : %s", MODEL)
    logger.info("  pending    : %d", total)
    logger.info("  threshold  : unique > %d OR foodie > %d → review queue",
                QUALIFY_THRESHOLD, QUALIFY_THRESHOLD)
    logger.info("  min rating : %.1f", min_rating)
    if dry_run:
        logger.info("  MODE       : DRY RUN (no API calls)")
    logger.info("=" * 60)

    stats = {"qualified": 0, "not_qualified": 0, "errors": 0}

    for i, (rid, place_id, name, address, rating, review_count, lat, lng, website) in enumerate(rows, 1):
        prefix = f"[{i:>3}/{total}]"

        if dry_run:
            note = _context_note(float(rating or 0), int(review_count or 0))
            logger.info("%s WOULD EVAL  %s  (%.1f★, %d reviews)%s",
                        prefix, name, float(rating or 0), int(review_count or 0),
                        f"  [{note}]" if note else "")
            continue

        logger.info("%s %s  (%.1f★, %d reviews)", prefix, name,
                    float(rating or 0), int(review_count or 0))
        try:
            scores   = call_gemini(name, address, rating, review_count, lat, lng, website)
            qualifies = save_prequalify(conn, place_id, scores["unique"], scores["foodie"])

            if qualifies:
                stats["qualified"] += 1
                logger.info("         -> ✓ QUALIFIES  unique=%d  foodie=%d  → review queue",
                            scores["unique"], scores["foodie"])
            else:
                stats["not_qualified"] += 1
                logger.info("         ->   skip        unique=%d  foodie=%d",
                            scores["unique"], scores["foodie"])

            time.sleep(0.3)
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    conn.close()
    logger.info("=" * 60)
    logger.info("Done.  Qualified: %d | Not qualified: %d | Errors: %d",
                stats["qualified"], stats["not_qualified"], stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Pre-qualify disqualified restaurants via Gemini")
    ap.add_argument("--limit",      type=int,   default=None, help="max restaurants to process")
    ap.add_argument("--min-rating", type=float, default=4.0,  help="minimum rating floor (default 4.0)")
    ap.add_argument("--dry-run",    action="store_true",       help="no API calls")
    args = ap.parse_args()

    run(limit=args.limit, min_rating=args.min_rating, dry_run=args.dry_run)

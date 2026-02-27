"""
mallorcaeat embedder — Step 4

Generates text embeddings (Gemini Embedding 001, SEMANTIC_SIMILARITY) for every
top restaurant and pre-computes 2-hour opening-time slots for scheduling similarity.

Text content concatenates all available text fields:
  description, types, categories, atmosphere, highlights, offerings,
  crowd, planning, summary_de, must_order, vibe

Embeddings are cached by place_id — each restaurant is embedded at most once.
Opening-time slots are stored in restaurants.open_slots (TEXT[]).

Usage:
    python embed.py                  # embed all unenriched top restaurants
    python embed.py --limit 10       # test with 10 restaurants
    python embed.py --dry-run        # show text content, no API calls
    python embed.py --force          # re-embed even if already cached
    python embed.py --batch 25       # smaller batches (default 50)
"""

import argparse
import json
import logging
import os
import time

import psycopg2.extras
from google import genai
from google.genai import types

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL      = "gemini-embedding-001"
BATCH_SIZE = 50  # texts per API call (stay well under quota)

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])


# ── Opening hours → 2-hour time slots ────────────────────────────────────────

_DAY_MAP = {
    "Montag":     "Mo",
    "Dienstag":   "Di",
    "Mittwoch":   "Mi",
    "Donnerstag": "Do",
    "Freitag":    "Fr",
    "Samstag":    "Sa",
    "Sonntag":    "So",
}


def _parse_time_range(s: str) -> list[int]:
    """'13:00–15:45' → list of 2-h block start-hours that overlap the range."""
    s = s.replace("\u2013", "-").replace("\u2014", "-")  # en-dash / em-dash → hyphen
    parts = s.split("-")
    if len(parts) != 2:
        return []
    try:
        sh  = int(parts[0].strip().split(":")[0])
        end = parts[1].strip()
        eh  = int(end.split(":")[0])
        em  = int(end.split(":")[1]) if ":" in end else 0
        if em > 0:
            eh += 1  # partial hour → next block may be touched
        return [h for h in range(0, 24, 2) if h < eh and h + 2 > sh]
    except (ValueError, IndexError):
        return []


def compute_open_slots(opening_hours: dict | None) -> list[str]:
    """Convert Serper opening_hours dict → sorted list of 'DayHH' slot strings.

    Example output: ['Fr12', 'Fr14', 'Fr18', 'Fr20', 'Fr22', 'Sa12', ...]
    Each slot = day abbreviation + 2-digit hour (start of 2-h block).
    """
    if not opening_hours:
        return []
    slots: set[str] = set()
    for day_de, hours_str in opening_hours.items():
        day = _DAY_MAP.get(day_de, day_de[:2])
        if not hours_str or hours_str.strip().lower() in ("geschlossen", "closed", ""):
            continue
        for rng in hours_str.split(","):
            rng = rng.strip()
            for bh in _parse_time_range(rng):
                slots.add(f"{day}{bh:02d}")
    return sorted(slots)


# ── Text content builder ──────────────────────────────────────────────────────

def build_text_content(row: dict) -> str:
    """Concatenate all available text fields into one string for embedding.

    Format: 'field: value\\nfield: value\\n...'
    Fields are only included when non-empty.
    Field order matches the user's request:
      description, types, categories, atmosphere, highlights,
      offerings, crowd, planning, summary_de, must_order, vibe
    """
    parts: list[str] = []

    raw = row.get("raw_data") or {}
    if isinstance(raw, str):
        raw = json.loads(raw)

    # ── Serper fields (from raw_data) ────────────────────────────────────────
    desc = (raw.get("description") or "").strip()
    if desc:
        parts.append(f"description: {desc}")

    types_list = [t for t in (raw.get("types") or []) if t]
    if types_list:
        parts.append(f"types: {', '.join(types_list)}")

    cats = [c for c in (row.get("categories") or []) if c]
    if cats:
        parts.append(f"categories: {', '.join(cats)}")

    # ── SerpAPI detail arrays ────────────────────────────────────────────────
    for field in ("atmosphere", "highlights", "offerings", "crowd", "planning"):
        val = [v for v in (row.get(field) or []) if v]
        if val:
            parts.append(f"{field}: {', '.join(val)}")

    # ── Gemini enrichment text ───────────────────────────────────────────────
    for field in ("summary_de", "must_order", "vibe"):
        val = (row.get(field) or "").strip()
        if val:
            parts.append(f"{field}: {val}")

    return "\n".join(parts)


# ── Database helpers ──────────────────────────────────────────────────────────

def ensure_schema(conn):
    """Create restaurant_embeddings table and open_slots column if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS restaurant_embeddings (
                place_id     TEXT PRIMARY KEY,
                text_content TEXT        NOT NULL,
                embedding    REAL[]      NOT NULL,
                model        TEXT        NOT NULL DEFAULT 'gemini-embedding-001',
                embedded_at  TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            ALTER TABLE restaurants
            ADD COLUMN IF NOT EXISTS open_slots TEXT[];
        """)
    conn.commit()


def fetch_pending(conn, limit: int | None, force: bool) -> list[dict]:
    join     = "" if force else "LEFT JOIN restaurant_embeddings emb ON emb.place_id = t.place_id"
    where    = "" if force else "AND emb.place_id IS NULL"
    limit_cl = "LIMIT %(limit)s" if limit else ""

    sql = f"""
        SELECT
            t.place_id,
            t.name,
            res.raw_data,
            res.categories,
            sd.atmosphere,
            sd.highlights,
            sd.offerings,
            sd.crowd,
            sd.planning,
            e.summary_de,
            e.must_order,
            e.vibe,
            res.raw_data->'openingHours' AS opening_hours
        FROM  top_restaurants      t
        {join}
        JOIN  restaurants          res ON res.place_id = t.place_id
        LEFT JOIN serpapi_details  sd  ON sd.place_id  = t.place_id
        LEFT JOIN gemini_enrichments e ON e.place_id   = t.place_id
        WHERE 1=1 {where}
        ORDER BY t.rating DESC, t.rating_count DESC
        {limit_cl}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"limit": limit} if limit else {})
        return [dict(r) for r in cur.fetchall()]


def save_embedding(
    conn,
    place_id:     str,
    text_content: str,
    vector:       list[float],
    open_slots:   list[str],
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO restaurant_embeddings (place_id, text_content, embedding, model)
            VALUES (%s, %s, %s::real[], %s)
            ON CONFLICT (place_id) DO UPDATE SET
                text_content = EXCLUDED.text_content,
                embedding    = EXCLUDED.embedding,
                model        = EXCLUDED.model,
                embedded_at  = NOW()
            """,
            (place_id, text_content, list(vector), MODEL),
        )
        # Always update open_slots (even on re-embed)
        cur.execute(
            "UPDATE restaurants SET open_slots = %s WHERE place_id = %s",
            (open_slots, place_id),
        )
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def run(limit=None, dry_run=False, force=False, batch_size=BATCH_SIZE):
    conn = get_connection()
    ensure_schema(conn)

    rows  = fetch_pending(conn, limit, force)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat embedder — Gemini Embedding 001")
    logger.info("  model      : %s", MODEL)
    logger.info("  pending    : %d", total)
    logger.info("  batch size : %d", batch_size)
    if dry_run:
        logger.info("  MODE       : DRY RUN (no API calls, no costs)")
    if force:
        logger.info("  MODE       : FORCE (re-embedding cached entries)")
    logger.info("=" * 60)

    if dry_run:
        for row in rows[:8]:
            text = build_text_content(row)
            oh   = row.get("opening_hours") or {}
            if isinstance(oh, str):
                oh = json.loads(oh)
            slots = compute_open_slots(oh)
            logger.info("WOULD EMBED  %s", row["name"])
            logger.info("  chars=%d  slots=%d", len(text), len(slots))
            logger.info("  %s", text[:160].replace("\n", " | "))
        conn.close()
        return

    stats = {"ok": 0, "empty": 0, "errors": 0}

    for batch_start in range(0, total, batch_size):
        batch  = rows[batch_start: batch_start + batch_size]
        texts:  list[str]  = []
        valid:  list[dict] = []

        for row in batch:
            text = build_text_content(row)
            if not text.strip():
                logger.warning("  SKIP (no text content): %s", row["name"])
                stats["empty"] += 1
                continue
            texts.append(text)
            valid.append(row)

        if not texts:
            continue

        logger.info(
            "Batch %d–%d / %d …",
            batch_start + 1,
            batch_start + len(texts),
            total,
        )

        try:
            result = client.models.embed_content(
                model=MODEL,
                contents=texts,
                config=types.EmbedContentConfig(task_type="SEMANTIC_SIMILARITY"),
            )
            for row, emb_obj, text in zip(valid, result.embeddings, texts):
                pid    = row["place_id"]
                vector = emb_obj.values  # list[float]

                oh = row.get("opening_hours") or {}
                if isinstance(oh, str):
                    oh = json.loads(oh)
                open_slots = compute_open_slots(oh)

                save_embedding(conn, pid, text, vector, open_slots)
                logger.info(
                    "  ✓  %-45s  dim=%d  slots=%d",
                    row["name"][:45], len(vector), len(open_slots),
                )
                stats["ok"] += 1

            time.sleep(0.5)  # gentle pacing between batches

        except Exception as exc:
            logger.error("  ✗  Batch error: %s", exc)
            stats["errors"] += len(texts)

    conn.close()
    logger.info("=" * 60)
    logger.info(
        "Done.  OK: %d | Empty (skipped): %d | Errors: %d",
        stats["ok"], stats["empty"], stats["errors"],
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Embed restaurants for similarity search")
    ap.add_argument("--limit",   type=int,  default=None,  help="max restaurants to process")
    ap.add_argument("--dry-run", action="store_true",      help="no API calls, show text content")
    ap.add_argument("--force",   action="store_true",      help="re-embed even if already cached")
    ap.add_argument("--batch",   type=int,  default=BATCH_SIZE, help="texts per API call")
    args = ap.parse_args()

    run(
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
        batch_size=args.batch,
    )

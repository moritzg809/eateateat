"""
mallorcaeat Jina semantic embedder

Generates Jina v3 embeddings (jinaai/jina-embeddings-v3, 1024 dims) for every
complete restaurant and stores them in restaurant_embeddings.jina_embedding.

The embedding text is built as a key=value string covering all available fields:
    name, küche, vibe, zusammenfassung, bestellen, cuisine_tags, interior, food,
    publikum, preis, bewertung, preisstufe, atmosphäre, highlights, angebote,
    beliebt_für, crowd, all 11 vibe-scores (only if > 0)

This allows Flask to do semantic search at query time:
    query → model.encode(task='retrieval.query') → cosine sim → top-N place_ids

Embeddings are incremental: restaurants without jina_embedding are processed first.
Run after completeness/details/classify so all fields are populated.

Usage:
    python jina_embed.py                 # embed all top restaurants without Jina vector
    python jina_embed.py --limit 10      # test with 10 restaurants
    python jina_embed.py --dry-run       # show key=value text, no API calls
    python jina_embed.py --force         # re-embed even if already cached
    python jina_embed.py --batch 64      # batch size (default 32)
"""

import argparse
import logging
import time

import numpy as np
import psycopg2.extras

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL      = "jinaai/jina-embeddings-v5-text-small-retrieval"
BATCH_SIZE = 32

# Score keys and their German labels (only included when score > 0)
_SCORE_KEYS = [
    ("family_score",   "score_family"),
    ("date_score",     "score_date"),
    ("friends_score",  "score_friends"),
    ("solo_score",     "score_solo"),
    ("relaxed_score",  "score_relaxed"),
    ("party_score",    "score_party"),
    ("special_score",  "score_special"),
    ("foodie_score",   "score_foodie"),
    ("lingering_score","score_lingering"),
    ("unique_score",   "score_unique"),
    ("dresscode_score","score_dresscode"),
]

_PRICE_LEVEL_MAP = {
    "$":    "günstig",
    "$$":   "mittel",
    "$$$":  "gehoben",
    "$$$$": "luxus",
}


# ── Text builder ──────────────────────────────────────────────────────────────

def build_jina_text(row: dict) -> str:
    """Build a rich key=value embedding text from all available restaurant fields.

    Only keys with non-empty, non-null values are included.
    Format: 'key=value\\nkey2=value2\\n...'
    """
    parts: list[str] = []

    def _add(key: str, value) -> None:
        """Add key=value if value is non-empty."""
        if value is None:
            return
        if isinstance(value, (list, tuple)):
            s = ", ".join(str(v) for v in value if v)
            if s:
                parts.append(f"{key}={s}")
        elif isinstance(value, (int, float)):
            if value:
                parts.append(f"{key}={value}")
        else:
            s = str(value).strip()
            if s and s.lower() not in ("none", "null", "n/a", "-"):
                parts.append(f"{key}={s}")

    # ── Core restaurant fields ────────────────────────────────────────────────
    _add("name",      row.get("name"))
    _add("adresse",   row.get("address"))

    rating = row.get("rating")
    rating_count = row.get("rating_count")
    if rating:
        if rating_count:
            _add("bewertung", f"{rating} ({rating_count} Stimmen)")
        else:
            _add("bewertung", str(rating))

    price_level = row.get("price_level")
    if price_level:
        label = _PRICE_LEVEL_MAP.get(price_level, price_level)
        _add("preisstufe", label)

    # ── Gemini enrichment ─────────────────────────────────────────────────────
    _add("küche",          row.get("cuisine_type"))
    _add("vibe",           row.get("vibe"))
    _add("zusammenfassung",row.get("summary_de"))
    _add("bestellen",      row.get("must_order"))
    _add("cuisine_tags",   row.get("cuisine_tags"))
    _add("interior",       row.get("interior_tags"))
    _add("food",           row.get("food_tags"))

    audience = row.get("audience_type")
    if audience:
        _add("publikum", audience)

    avg_price = row.get("avg_price_pp")
    if avg_price:
        _add("preis", f"{avg_price}€")

    # ── Vibe scores (only non-zero values) ───────────────────────────────────
    for db_col, label in _SCORE_KEYS:
        val = row.get(db_col)
        if val:
            _add(label, int(val))

    # ── SerpAPI detail arrays ─────────────────────────────────────────────────
    _add("atmosphäre",   row.get("atmosphere"))
    _add("highlights",   row.get("highlights"))
    _add("angebote",     row.get("offerings"))
    _add("crowd",        row.get("crowd"))
    _add("beliebt_für",  row.get("popular_for"))

    return "\n".join(parts)


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn) -> None:
    """Apply migration 012 if jina columns don't exist yet."""
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE restaurant_embeddings
                ADD COLUMN IF NOT EXISTS jina_text        TEXT,
                ADD COLUMN IF NOT EXISTS jina_embedding   REAL[],
                ADD COLUMN IF NOT EXISTS jina_model       TEXT,
                ADD COLUMN IF NOT EXISTS jina_embedded_at TIMESTAMPTZ;
        """)
    conn.commit()


# ── DB helpers ────────────────────────────────────────────────────────────────

def fetch_pending(conn, limit: int | None, force: bool) -> list[dict]:
    """Fetch complete restaurants that need Jina embedding."""
    jina_filter = "" if force else "AND re.jina_embedding IS NULL"
    limit_cl    = "LIMIT %(limit)s" if limit else ""

    sql = f"""
        SELECT
            t.place_id,
            res.name,
            res.address,
            res.rating,
            res.rating_count,
            res.price_level,
            e.cuisine_type,
            e.vibe,
            e.summary_de,
            e.must_order,
            e.cuisine_tags,
            e.interior_tags,
            e.food_tags,
            e.audience_type,
            e.avg_price_pp,
            e.family_score,
            e.date_score,
            e.friends_score,
            e.solo_score,
            e.relaxed_score,
            e.party_score,
            e.special_score,
            e.foodie_score,
            e.lingering_score,
            e.unique_score,
            e.dresscode_score,
            sd.atmosphere,
            sd.highlights,
            sd.offerings,
            sd.crowd,
            sd.popular_for
        FROM  top_restaurants      t
        JOIN  restaurants          res ON res.place_id = t.place_id
        LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
        LEFT JOIN serpapi_details   sd  ON sd.place_id = t.place_id
        LEFT JOIN restaurant_embeddings re ON re.place_id = t.place_id
        WHERE 1=1 {jina_filter}
        ORDER BY t.rating DESC, t.rating_count DESC
        {limit_cl}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"limit": limit} if limit else {})
        return [dict(r) for r in cur.fetchall()]


def save_embeddings(
    conn,
    rows:    list[dict],
    vectors: np.ndarray,
    texts:   list[str],
) -> None:
    """UPSERT Jina embeddings for a batch of restaurants."""
    with conn.cursor() as cur:
        for row, vector, text in zip(rows, vectors, texts):
            cur.execute(
                """
                INSERT INTO restaurant_embeddings (place_id, text_content, embedding, model)
                VALUES (%(pid)s, '', '{}'::real[], 'none')
                ON CONFLICT (place_id) DO NOTHING;

                UPDATE restaurant_embeddings SET
                    jina_text        = %(jina_text)s,
                    jina_embedding   = %(jina_embedding)s,
                    jina_model       = %(jina_model)s,
                    jina_embedded_at = NOW()
                WHERE place_id = %(pid)s;
                """,
                {
                    "pid":           row["place_id"],
                    "jina_text":     text,
                    "jina_embedding": list(float(v) for v in vector),
                    "jina_model":    MODEL,
                },
            )
    conn.commit()


# ── Model ─────────────────────────────────────────────────────────────────────

def load_model():
    logger.info("Loading Jina model: %s (first run downloads ~300 MB)", MODEL)
    t0 = time.time()
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415
    model = SentenceTransformer(MODEL, trust_remote_code=True)
    logger.info("Model loaded in %.1fs", time.time() - t0)
    return model


# ── Main ──────────────────────────────────────────────────────────────────────

def run(limit=None, dry_run=False, force=False, batch_size=BATCH_SIZE):
    conn = get_connection()
    ensure_schema(conn)

    rows  = fetch_pending(conn, limit, force)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat Jina semantic embedder")
    logger.info("  model      : %s", MODEL)
    logger.info("  pending    : %d", total)
    logger.info("  batch size : %d", batch_size)
    if dry_run:
        logger.info("  MODE       : DRY RUN (no API calls, no costs)")
    if force:
        logger.info("  MODE       : FORCE (re-embedding cached entries)")
    logger.info("=" * 60)

    if dry_run:
        for row in rows[:5]:
            text = build_jina_text(row)
            logger.info("WOULD EMBED  %s", row["name"])
            logger.info("  chars=%d", len(text))
            for line in text.split("\n"):
                logger.info("  %s", line)
            logger.info("")
        conn.close()
        return

    if total == 0:
        logger.info("Nothing to embed.")
        conn.close()
        return

    model = load_model()
    stats = {"ok": 0, "empty": 0, "errors": 0}

    for batch_start in range(0, total, batch_size):
        batch  = rows[batch_start: batch_start + batch_size]
        texts: list[str]  = []
        valid: list[dict] = []

        for row in batch:
            text = build_jina_text(row)
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
            min(batch_start + len(texts), total),
            total,
        )

        try:
            vectors = model.encode(
                texts,
                task="retrieval.passage",
                normalize_embeddings=True,
                batch_size=batch_size,
                show_progress_bar=False,
            )
            vectors = np.array(vectors, dtype=np.float32)

            save_embeddings(conn, valid, vectors, texts)

            for row in valid:
                logger.info("  ✓  %s", row["name"][:60])
                stats["ok"] += 1

        except Exception as exc:
            logger.error("  ✗  Batch error: %s", exc)
            stats["errors"] += len(texts)

    conn.close()
    logger.info("=" * 60)
    logger.info(
        "Done.  OK: %d | Skipped (empty): %d | Errors: %d",
        stats["ok"], stats["empty"], stats["errors"],
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Jina semantic embedder for restaurant search")
    ap.add_argument("--limit",   type=int,  default=None, help="max restaurants to process")
    ap.add_argument("--dry-run", action="store_true",     help="show text, no API calls")
    ap.add_argument("--force",   action="store_true",     help="re-embed even if cached")
    ap.add_argument("--batch",   type=int,  default=BATCH_SIZE, help="batch size (default 32)")
    args = ap.parse_args()

    run(
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
        batch_size=args.batch,
    )

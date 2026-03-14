"""
mallorcaeat attribute embedder

Embeds all distinct values from cuisine_tags, interior_tags, and food_tags using
Jina embeddings-v5-text-small-retrieval and pre-computes nearest neighbors.
Results are stored in the attr_neighbors table so Flask can expand ?attr_filter
queries without loading any model.

Usage:
    python attr_embed.py                   # embed + compute neighbors, write to DB
    python attr_embed.py --dry-run         # show neighbor assignments, no DB write
    python attr_embed.py --threshold 0.72  # stricter matching
    python attr_embed.py --top-k 8         # more neighbors per value
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

MODEL     = "jinaai/jina-embeddings-v5-text-small-retrieval"
THRESHOLD = 0.68
TOP_K     = 5


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS attr_neighbors (
                attr_value    TEXT        PRIMARY KEY,
                similar_attrs TEXT[]      NOT NULL DEFAULT '{}',
                embedded_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()


# ── Data loading ──────────────────────────────────────────────────────────────

def fetch_attr_values(conn) -> list[str]:
    """Return all distinct non-null values from cuisine_tags, interior_tags, food_tags."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT val
            FROM (
                SELECT unnest(cuisine_tags)  AS val FROM gemini_enrichments
                    WHERE cuisine_tags  IS NOT NULL
                UNION
                SELECT unnest(interior_tags) AS val FROM gemini_enrichments
                    WHERE interior_tags IS NOT NULL
                UNION
                SELECT unnest(food_tags)     AS val FROM gemini_enrichments
                    WHERE food_tags     IS NOT NULL
            ) sub
            WHERE val IS NOT NULL AND val != ''
            ORDER BY val
        """)
        return [row[0] for row in cur.fetchall()]


# ── Embedding + similarity ─────────────────────────────────────────────────────

def load_model():
    logger.info("Loading Jina model: %s", MODEL)
    t0 = time.time()
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415
    model = SentenceTransformer(MODEL, trust_remote_code=True)
    logger.info("Model loaded in %.1fs", time.time() - t0)
    return model


def embed_strings(model, texts: list[str]) -> np.ndarray:
    logger.info("Embedding %d attribute strings …", len(texts))
    vecs = model.encode(
        texts,
        task="text-matching",
        show_progress_bar=True,
        batch_size=64,
    )
    vecs = np.array(vecs, dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    return vecs / norms


def compute_neighbors(
    texts: list[str],
    matrix: np.ndarray,
    threshold: float,
    top_k: int,
) -> dict[str, list[str]]:
    sim = matrix @ matrix.T
    result: dict[str, list[str]] = {}
    for i, val in enumerate(texts):
        row = sim[i]
        neighbors = [
            (texts[j], float(row[j]))
            for j in range(len(texts))
            if j != i and row[j] >= threshold
        ]
        neighbors.sort(key=lambda x: x[1], reverse=True)
        result[val] = [n for n, _ in neighbors[:top_k]]
    return result


# ── DB write ──────────────────────────────────────────────────────────────────

def save_neighbors(conn, neighbors: dict[str, list[str]]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO attr_neighbors (attr_value, similar_attrs, embedded_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (attr_value) DO UPDATE SET
                similar_attrs = EXCLUDED.similar_attrs,
                embedded_at   = NOW()
            """,
            [(val, sims) for val, sims in neighbors.items()],
        )
    conn.commit()
    logger.info("Saved %d attr neighbor records to DB.", len(neighbors))


# ── Main ──────────────────────────────────────────────────────────────────────

def run(threshold: float = THRESHOLD, top_k: int = TOP_K, dry_run: bool = False) -> None:
    conn = get_connection()
    ensure_schema(conn)

    attr_values = fetch_attr_values(conn)
    if not attr_values:
        logger.warning("No attribute values found. Run enrich.py first.")
        conn.close()
        return

    logger.info("=" * 60)
    logger.info("mallorcaeat attribute embedder")
    logger.info("  model      : %s", MODEL)
    logger.info("  values     : %d unique attribute values", len(attr_values))
    logger.info("  threshold  : %.2f", threshold)
    logger.info("  top_k      : %d", top_k)
    if dry_run:
        logger.info("  MODE       : DRY RUN (no DB write)")
    logger.info("=" * 60)

    model  = load_model()
    matrix = embed_strings(model, attr_values)
    neighbors = compute_neighbors(attr_values, matrix, threshold, top_k)

    values_with_neighbors = sum(1 for v in neighbors.values() if v)
    values_solo           = len(attr_values) - values_with_neighbors

    logger.info("")
    logger.info("─── Neighbor assignments ───────────────────────────────")
    for val in attr_values[:80]:
        sims = neighbors.get(val, [])
        if sims:
            logger.info("  %-40s  →  %s", val, ",  ".join(sims))
        else:
            logger.info("  %-40s  →  (no neighbors above threshold)", val)

    if len(attr_values) > 80:
        logger.info("  … (%d more not shown)", len(attr_values) - 80)

    logger.info("")
    logger.info("Summary: %d values with neighbors, %d isolated", values_with_neighbors, values_solo)
    logger.info("=" * 60)

    if not dry_run:
        save_neighbors(conn, neighbors)
    else:
        logger.info("DRY RUN — nothing written to DB.")

    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Embed attribute tags and compute Jina neighbors")
    ap.add_argument("--threshold", type=float, default=THRESHOLD,
                    help=f"cosine similarity threshold (default {THRESHOLD})")
    ap.add_argument("--top-k",     type=int,   default=TOP_K,
                    help=f"max neighbors per value (default {TOP_K})")
    ap.add_argument("--dry-run",   action="store_true",
                    help="show neighbor assignments without writing to DB")
    args = ap.parse_args()

    run(threshold=args.threshold, top_k=args.top_k, dry_run=args.dry_run)

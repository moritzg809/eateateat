"""
mallorcaeat cuisine embedder

Embeds all distinct cuisine_type strings using Jina embeddings-v5-text-small-retrieval
and pre-computes nearest neighbors per type. Results are stored in the
cuisine_neighbors table so Flask can expand filters without loading any model.

Usage:
    python cuisine_embed.py                   # embed + compute neighbors, write to DB
    python cuisine_embed.py --dry-run         # show neighbor assignments, no DB write
    python cuisine_embed.py --threshold 0.72  # stricter matching
    python cuisine_embed.py --top-k 8         # more neighbors per type
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
THRESHOLD = 0.68   # cosine similarity threshold for "similar enough"
TOP_K     = 5      # max neighbors per cuisine_type


# ── Schema ────────────────────────────────────────────────────────────────────

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cuisine_neighbors (
                cuisine_type   TEXT PRIMARY KEY,
                similar_types  TEXT[]      NOT NULL DEFAULT '{}',
                embedded_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()


# ── Data loading ──────────────────────────────────────────────────────────────

def fetch_cuisine_types(conn) -> list[str]:
    """Return all distinct non-null cuisine_type values, ordered by frequency."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cuisine_type
            FROM gemini_enrichments
            WHERE cuisine_type IS NOT NULL AND cuisine_type != ''
            GROUP BY cuisine_type
            ORDER BY COUNT(*) DESC
        """)
        return [row[0] for row in cur.fetchall()]


# ── Embedding + similarity ─────────────────────────────────────────────────────

def load_model():
    """Load Jina model via sentence-transformers (downloads on first run ~350 MB)."""
    logger.info("Loading Jina model: %s", MODEL)
    t0 = time.time()
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415
    model = SentenceTransformer(MODEL, trust_remote_code=True)
    logger.info("Model loaded in %.1fs", time.time() - t0)
    return model


def embed_strings(model, texts: list[str]) -> np.ndarray:
    """Embed a list of strings and return L2-normalised matrix (N × dim)."""
    logger.info("Embedding %d cuisine type strings …", len(texts))
    vecs = model.encode(
        texts,
        task="text-matching",   # symmetric: comparing strings to strings
        show_progress_bar=True,
        batch_size=64,
    )
    vecs = np.array(vecs, dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)   # avoid division by zero
    return vecs / norms


def compute_neighbors(
    texts: list[str],
    matrix: np.ndarray,
    threshold: float,
    top_k: int,
) -> dict[str, list[str]]:
    """Compute top-K nearest neighbors above threshold for each cuisine_type.

    Returns {cuisine_type: [neighbor1, neighbor2, ...]}
    """
    # Full pairwise cosine similarity (since vectors are already normalised)
    sim = matrix @ matrix.T   # shape (N, N)

    result: dict[str, list[str]] = {}
    for i, ct in enumerate(texts):
        row = sim[i]
        # Exclude self (exact match = 1.0), keep only above threshold
        neighbors = [
            (texts[j], float(row[j]))
            for j in range(len(texts))
            if j != i and row[j] >= threshold
        ]
        # Sort by score descending, take top_k
        neighbors.sort(key=lambda x: x[1], reverse=True)
        result[ct] = [n for n, _ in neighbors[:top_k]]

    return result


# ── DB write ──────────────────────────────────────────────────────────────────

def save_neighbors(conn, neighbors: dict[str, list[str]]) -> None:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO cuisine_neighbors (cuisine_type, similar_types, embedded_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (cuisine_type) DO UPDATE SET
                similar_types = EXCLUDED.similar_types,
                embedded_at   = NOW()
            """,
            [(ct, sims) for ct, sims in neighbors.items()],
        )
    conn.commit()
    logger.info("Saved %d cuisine_type neighbor records to DB.", len(neighbors))


# ── Main ──────────────────────────────────────────────────────────────────────

def run(threshold: float = THRESHOLD, top_k: int = TOP_K, dry_run: bool = False) -> None:
    conn = get_connection()
    ensure_schema(conn)

    cuisine_types = fetch_cuisine_types(conn)
    if not cuisine_types:
        logger.warning("No cuisine_type values found in gemini_enrichments. Run enrich.py first.")
        conn.close()
        return

    logger.info("=" * 60)
    logger.info("mallorcaeat cuisine embedder")
    logger.info("  model      : %s", MODEL)
    logger.info("  types      : %d unique cuisine_type values", len(cuisine_types))
    logger.info("  threshold  : %.2f", threshold)
    logger.info("  top_k      : %d", top_k)
    if dry_run:
        logger.info("  MODE       : DRY RUN (no DB write)")
    logger.info("=" * 60)

    model  = load_model()
    matrix = embed_strings(model, cuisine_types)
    neighbors = compute_neighbors(cuisine_types, matrix, threshold, top_k)

    # ── Print results ────────────────────────────────────────────────────────
    types_with_neighbors = sum(1 for v in neighbors.values() if v)
    types_solo           = len(cuisine_types) - types_with_neighbors

    logger.info("")
    logger.info("─── Neighbor assignments ───────────────────────────────")
    for ct in cuisine_types[:60]:   # show first 60 in log
        sims = neighbors.get(ct, [])
        if sims:
            sim_str = ",  ".join(sims)
            logger.info("  %-40s  →  %s", ct, sim_str)
        else:
            logger.info("  %-40s  →  (no neighbors above threshold)", ct)

    if len(cuisine_types) > 60:
        logger.info("  … (%d more not shown)", len(cuisine_types) - 60)

    logger.info("")
    logger.info("Summary: %d types with neighbors, %d isolated", types_with_neighbors, types_solo)
    logger.info("=" * 60)

    if not dry_run:
        save_neighbors(conn, neighbors)
    else:
        logger.info("DRY RUN — nothing written to DB.")

    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Embed cuisine types and compute Jina neighbors")
    ap.add_argument("--threshold", type=float, default=THRESHOLD,
                    help=f"cosine similarity threshold (default {THRESHOLD})")
    ap.add_argument("--top-k",     type=int,   default=TOP_K,
                    help=f"max neighbors per cuisine_type (default {TOP_K})")
    ap.add_argument("--dry-run",   action="store_true",
                    help="show neighbor assignments without writing to DB")
    args = ap.parse_args()

    run(threshold=args.threshold, top_k=args.top_k, dry_run=args.dry_run)

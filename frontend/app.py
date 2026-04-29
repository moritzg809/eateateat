import json
import math
import os
import re
import time as _time
import urllib.parse
import urllib.request
import uuid
from urllib.parse import urlencode

import markdown2
import numpy as np
import psycopg2
import psycopg2.extras
import jwt as pyjwt
from flask import Flask, render_template, request, jsonify, make_response, redirect, session, g, url_for, abort

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mallorcaeat-dev-key")

SSO_SECRET = os.environ.get("SSO_SECRET", "")
PORTAL_URL = "https://moritzglauner.com"


@app.before_request
def require_auth():
    if request.path == "/sso" or request.path.startswith("/static"):
        return
    if not session.get("authenticated"):
        return redirect(PORTAL_URL)


@app.route("/sso")
def sso():
    token = request.args.get("token", "")
    try:
        pyjwt.decode(token, SSO_SECRET, algorithms=["HS256"])
        session["authenticated"] = True
        return redirect(url_for("index"))
    except Exception:
        return redirect(PORTAL_URL)

PHOTOS_DIR  = "/app/static/photos"   # Docker volume mount (same as scraper's /photos)
MAX_PHOTOS  = 8
PER_PAGE    = 24

PROFILES = [
    ("family",   "👨‍👩‍👧", "Familie"),
    ("date",     "💑",     "Date Night"),
    ("friends",  "👯",     "Friends Trip"),
    ("solo",     "🧍",     "Solo"),
    ("relaxed",  "😌",     "Entspannt"),
    ("special",  "✨",     "Besonderer Anlass"),
    ("foodie",   "🍽️",    "Foodie"),
    ("lingering","☕",     "Verweilen"),
    ("unique",   "💎",     "Geheimtipp"),
    ("outdoor",  "🏡",     "Terrasse"),
    ("view",     "🌅",     "Aussicht"),
]


def _extract_city(address: str | None) -> str | None:
    """Extract city name from a Mallorca address string."""
    if not address:
        return None
    m = re.search(r'0\d{4}\s+([^,]+)', address)
    return m.group(1).strip() if m else None


_TYPE_MAP = [
    # Bars / drinks first (most specific)
    (("cocktailbar",),                              "🍸", "Cocktailbar"),
    (("sportsbar",),                                "📺", "Sportsbar"),
    (("tapasbar",),                                 "🫒", "Tapasbar"),
    (("espressobar", "stehbar"),                    "☕", "Bar"),
    (("weinstube", "weinhandlung", "weinkellerei",
      "weingroß", "weinberg"),                      "🍷", "Weinbar"),
    (("bar",),                                      "🍸", "Bar"),
    (("brauerei",),                                 "🍺", "Brauerei"),
    # Cafés / coffee / breakfast
    (("café", "cafe", "coffeeshop",
      "kaffeeröster", "kaffeestand",
      "frühstückslokal", "brunch"),                 "☕", "Café"),
    (("konditorei", "tortenbäck", "bäckerei",
      "backerei"),                                  "🥐", "Bäckerei"),
    (("eiscafé", "frozen-yogurt"),                  "🍦", "Eiscafé"),
    (("teehaus",),                                  "🍵", "Teehaus"),
    # Specific food types
    (("sushi", "japanisch"),                        "🍣", "Sushi"),
    (("pizza", "pizzeria"),                         "🍕", "Pizzeria"),
    (("burger",),                                   "🍔", "Burger"),
    (("tapas",),                                    "🫒", "Tapas"),
    (("meeresfrüchte", "austern", "fisch"),         "🐟", "Seafood"),
    (("vegetarisch", "vegan", "naturkost"),         "🥗", "Vegetarisch"),
    (("asador", "grill", "chophouse", "churreria",
      "argentin", "brasilian"),                     "🥩", "Grill"),
    (("gourmet",),                                  "⭐", "Gourmet"),
    (("italienisch",),                              "🍝", "Italienisch"),
    (("spanisch", "mallorquin"),                    "🇪🇸", "Spanisch"),
    (("mediterran",),                               "🌊", "Mediterran"),
    (("indien", "indisch", "thai", "nepales",
      "peru", "südostasiat"),                       "🍜", "Asiatisch/Int."),
    (("imbiss",),                                   "🌮", "Imbiss"),
    (("bistro",),                                   "🥂", "Bistro"),
]


def _classify_type(raw_type: str | None) -> tuple[str, str]:
    """Return (emoji, short_label) for a Google Maps place type string."""
    if not raw_type:
        return ("🍽️", "Restaurant")
    t = raw_type.lower()
    for keywords, emoji, label in _TYPE_MAP:
        if any(kw in t for kw in keywords):
            return (emoji, label)
    return ("🍽️", "Restaurant")


def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ── DB initialisation (run once per process) ────────────────────────────────
_db_ready = False

def _init_db():
    global _db_ready
    if _db_ready:
        return
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_favorites (
                id          SERIAL PRIMARY KEY,
                session_id  TEXT    NOT NULL,
                place_id    TEXT    NOT NULL,
                list_type   TEXT    NOT NULL CHECK (list_type IN ('want', 'been')),
                score       INTEGER CHECK (score BETWEEN 0 AND 100),
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (session_id, place_id)
            );
            CREATE INDEX IF NOT EXISTS idx_user_favorites_session
                ON user_favorites(session_id);
        """)
        conn.commit()
    conn.close()
    _db_ready = True


# ── Jina semantic search ──────────────────────────────────────────────────────

_JINA_MODEL = None   # lazy-loaded on first search request

def _get_jina_model():
    """Load Jina v5 model on first call; cache globally for the process lifetime."""
    global _JINA_MODEL
    if _JINA_MODEL is None:
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        _JINA_MODEL = SentenceTransformer(
            "jinaai/jina-embeddings-v5-text-small-retrieval",
            trust_remote_code=True,
        )
    return _JINA_MODEL


_SEARCH_EMBED_CACHE: dict[int, tuple] = {}  # city_id → (place_ids, matrix, loaded_at)
_SEARCH_EMBED_TTL = 600  # 10 min


def _load_search_embeddings(conn, city_id: int):
    """Return (place_ids, L2-normalised matrix) for a city, cached 10 min.

    Returns None if no Jina embeddings exist yet (falls back to ILIKE search).
    """
    now = _time.time()
    cached = _SEARCH_EMBED_CACHE.get(city_id)
    if cached and now - cached[2] < _SEARCH_EMBED_TTL:
        return cached[0], cached[1]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT re.place_id, re.jina_embedding
            FROM   restaurant_embeddings re
            JOIN   top_restaurants t ON t.place_id = re.place_id
            WHERE  t.city_id = %s
              AND  re.jina_embedding IS NOT NULL
            ORDER BY t.rating DESC
            """,
            (city_id,),
        )
        rows = cur.fetchall()

    if not rows:
        return None

    place_ids = [r["place_id"] for r in rows]
    matrix = np.array([r["jina_embedding"] for r in rows], dtype=np.float32)

    # L2-normalise (vectors should already be normalised from scraper, but be safe)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    matrix = matrix / np.where(norms == 0, 1.0, norms)

    _SEARCH_EMBED_CACHE[city_id] = (place_ids, matrix, now)
    return place_ids, matrix


# ── City cache ────────────────────────────────────────────────────────────────
_CITY_CACHE: tuple | None = None
_CITY_CACHE_TTL = 600  # seconds


def _load_cities(conn) -> dict[str, dict]:
    """Return {slug: city_row} for all published cities. Cached for 10 min."""
    global _CITY_CACHE
    now = _time.time()
    if _CITY_CACHE and now - _CITY_CACHE[0] < _CITY_CACHE_TTL:
        return _CITY_CACHE[1]
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, slug, name, emoji, subtitle, country_code, sort_order
            FROM cities
            WHERE is_published = TRUE
            ORDER BY sort_order
        """)
        result = {row["slug"]: dict(row) for row in cur.fetchall()}
    _CITY_CACHE = (now, result)
    return result


def _get_city_or_404(city_slug: str) -> dict:
    """Look up a published city by slug, abort(404) if not found."""
    conn = get_db()
    try:
        cities = _load_cities(conn)
    finally:
        conn.close()
    city = cities.get(city_slug)
    if not city:
        abort(404)
    return city


# ── Favorites API ────────────────────────────────────────────────────────────

@app.route("/api/favorite", methods=["POST"])
def api_add_favorite():
    sid = request.cookies.get("sid")
    if not sid:
        return jsonify({"error": "no session"}), 400
    data      = request.get_json(silent=True) or {}
    place_id  = data.get("place_id", "").strip()
    list_type = data.get("list_type", "")
    score     = data.get("score")  # int or None
    if not place_id or list_type not in ("want", "been"):
        return jsonify({"error": "invalid data"}), 400
    if score is not None:
        score = max(0, min(100, int(score)))
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO user_favorites (session_id, place_id, list_type, score)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (session_id, place_id)
            DO UPDATE SET list_type = EXCLUDED.list_type,
                          score     = EXCLUDED.score
        """, (sid, place_id, list_type, score))
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/favorite/<place_id>", methods=["DELETE"])
def api_remove_favorite(place_id):
    sid = request.cookies.get("sid")
    if not sid:
        return jsonify({"error": "no session"}), 400
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM user_favorites WHERE session_id = %s AND place_id = %s",
            (sid, place_id)
        )
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── Cuisine-neighbor cache (module-level, refreshed every 10 min) ────────────

_CUISINE_CACHE: tuple[float, dict[str, list[str]]] | None = None
_ATTR_CACHE:    tuple[float, dict[str, list[str]]] | None = None
_CUISINE_CACHE_TTL = 600  # seconds


def _load_cuisine_neighbors(conn) -> dict[str, list[str]]:
    """Return {cuisine_type: [similar_type, …]} from cuisine_neighbors table.

    Returns an empty dict if the table doesn't exist or is empty.
    Cached module-wide for _CUISINE_CACHE_TTL seconds.
    """
    global _CUISINE_CACHE
    now = _time.time()
    if _CUISINE_CACHE and now - _CUISINE_CACHE[0] < _CUISINE_CACHE_TTL:
        return _CUISINE_CACHE[1]
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cuisine_type, similar_types FROM cuisine_neighbors"
            )
            result = {row[0]: (row[1] or []) for row in cur.fetchall()}
    except Exception:
        result = {}
    _CUISINE_CACHE = (now, result)
    return result


def _load_attr_neighbors(conn) -> dict[str, list[str]]:
    """Return {attr_value: [similar_attr, …]} from attr_neighbors table."""
    global _ATTR_CACHE
    now = _time.time()
    if _ATTR_CACHE and now - _ATTR_CACHE[0] < _CUISINE_CACHE_TTL:
        return _ATTR_CACHE[1]
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT attr_value, similar_attrs FROM attr_neighbors")
            result = {row[0]: (row[1] or []) for row in cur.fetchall()}
    except Exception:
        result = {}
    _ATTR_CACHE = (now, result)
    return result


def _cuisine_words(ct: str) -> frozenset[str]:
    """Tokenise a cuisine_type string into a frozenset of lowercase words."""
    return frozenset(
        w for w in ct.lower()
        .replace("-", " ").replace("&", " ").replace(",", " ").replace("/", " ")
        .split()
        if len(w) > 2   # drop very short tokens like "a", "de"
    )


def _cuisine_covers(existing: str, candidate: str, neighbors: dict) -> bool:
    """Return True if *candidate* is semantically redundant given *existing*.

    Two criteria (either is sufficient):
    1. Explicit Jina-neighbor relationship (bidirectional).
    2. Word-overlap: all words of the shorter token-set appear in the longer
       → e.g. "Japanisch" ⊆ "Japanisch-Fusion", "Mediterran" ⊆ "Modern-Mediterran"
    """
    # Explicit neighbor check (bidirectional)
    if candidate in neighbors.get(existing, []):
        return True
    if existing in neighbors.get(candidate, []):
        return True
    # Word-overlap heuristic
    w1, w2 = _cuisine_words(existing), _cuisine_words(candidate)
    if not w1 or not w2:
        return False
    shorter, longer = (w1, w2) if len(w1) <= len(w2) else (w2, w1)
    return len(shorter & longer) >= len(shorter)


def _load_top_cuisines(conn, limit: int = 20, city_id: int | None = None) -> list[tuple[str, int]]:
    """Return the top-N cuisine filter options for a given city.

    Tries city_cuisine_labels first (pre-computed wPMI + clustering).
    Falls back to the old global approach when the table is empty or missing.
    """
    # ── Fast path: pre-computed city DNA ──────────────────────────────────────
    if city_id is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT label, restaurant_n
                    FROM city_cuisine_labels
                    WHERE city_id = %(city_id)s
                    ORDER BY wpmi DESC
                    LIMIT %(limit)s
                    """,
                    {"city_id": city_id, "limit": limit},
                )
                rows = cur.fetchall()
            if rows:
                return [(row[0], row[1]) for row in rows]
        except Exception:
            pass   # table doesn't exist yet → fall through to legacy path

    # ── Legacy fallback: global top cuisines with greedy dedup ────────────────
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.cuisine_type, COUNT(*) AS n
                FROM top_restaurants t
                JOIN gemini_enrichments e ON e.place_id = t.place_id
                WHERE e.cuisine_type IS NOT NULL AND e.cuisine_type != ''
                  AND (%(city_id)s IS NULL OR t.city_id = %(city_id)s)
                  AND EXISTS (
                      SELECT 1 FROM cuisine_neighbors cn
                      WHERE cn.cuisine_type = e.cuisine_type
                  )
                GROUP BY e.cuisine_type
                ORDER BY n DESC
                LIMIT %(limit)s
                """,
                {"city_id": city_id, "limit": limit * 5},
            )
            candidates = [(row[0], row[1]) for row in cur.fetchall()]
    except Exception:
        return []

    neighbors = _load_cuisine_neighbors(conn)

    # Greedy deduplication: keep a type only if no already-selected type covers it
    selected: list[tuple[str, int]] = []
    for ctype, count in candidates:
        if any(_cuisine_covers(sel, ctype, neighbors) for sel, _ in selected):
            continue
        selected.append((ctype, count))
        if len(selected) >= limit:
            break

    return selected


# ── Recommender — candidate cache (module-level, refreshed every 5 min) ──────

_CAND_CACHE: dict[int, tuple] = {}  # key: city_id
_CAND_CACHE_TTL = 300  # seconds

_SCORE_COLS = [
    "family_score",  "date_score",    "friends_score", "solo_score",
    "relaxed_score", "special_score", "foodie_score",  "lingering_score",
    "unique_score",  "outdoor_score", "view_score",
]
# Critic sub-scores — separate 4-dim vector (76 % coverage, normalised independently)
_CRITIC_COLS = ["cuisine_score", "service_score", "value_score", "ambiance_score"]


def _load_candidates(conn, city_id: int | None = None) -> list[dict]:
    """Load all top restaurants with scores, embeddings and open_slots.

    Results are cached module-wide for _CAND_CACHE_TTL seconds so repeated
    calls to /api/similar don't hammer the DB with 6 MB queries.
    Each row is pre-processed:
      row['scores_norm']  – normalised numpy score vector (or None)
      row['emb_norm']     – normalised embedding vector   (or None)
    """
    global _CAND_CACHE
    cache_key = city_id if city_id is not None else -1
    now = _time.time()
    if cache_key in _CAND_CACHE and now - _CAND_CACHE[cache_key][0] < _CAND_CACHE_TTL:
        return _CAND_CACHE[cache_key][1]

    city_filter = "AND r.city_id = %(city_id)s" if city_id is not None else ""
    city_params = {"city_id": city_id} if city_id is not None else {}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                r.place_id,
                r.name,
                r.rating,
                r.price_level,
                r.thumbnail_url,
                r.city_id,
                res.raw_data->>'type'  AS place_type,
                res.open_slots,
                COALESCE(e.family_score,    0) AS family_score,
                COALESCE(e.date_score,      0) AS date_score,
                COALESCE(e.friends_score,   0) AS friends_score,
                COALESCE(e.solo_score,      0) AS solo_score,
                COALESCE(e.relaxed_score,   0) AS relaxed_score,
                COALESCE(e.party_score,     0) AS party_score,
                COALESCE(e.special_score,   0) AS special_score,
                COALESCE(e.foodie_score,    0) AS foodie_score,
                COALESCE(e.lingering_score, 0) AS lingering_score,
                COALESCE(e.unique_score,    0) AS unique_score,
                COALESCE(e.dresscode_score, 0) AS dresscode_score,
                COALESCE(e.outdoor_score,   0) AS outdoor_score,
                COALESCE(e.view_score,      0) AS view_score,
                COALESCE(e.cuisine_score,   0) AS cuisine_score,
                COALESCE(e.service_score,   0) AS service_score,
                COALESCE(e.value_score,     0) AS value_score,
                COALESCE(e.ambiance_score,  0) AS ambiance_score,
                e.avg_price_pp,
                e.audience_type,
                e.cuisine_tags,
                e.interior_tags,
                e.food_tags,
                e.vibe,
                emb.embedding,
                COALESCE(sd.atmosphere, '{{}}') || COALESCE(sd.offerings, '{{}}') ||
                COALESCE(sd.crowd,      '{{}}') || COALESCE(sd.highlights,'{{}}') AS tags
            FROM  top_restaurants           r
            JOIN  restaurants             res ON res.place_id = r.place_id
            LEFT JOIN gemini_enrichments    e ON e.place_id   = r.place_id
            LEFT JOIN restaurant_embeddings emb ON emb.place_id = r.place_id
            LEFT JOIN serpapi_details      sd  ON sd.place_id  = r.place_id
            WHERE 1=1 {city_filter}
        """, city_params)
        raw_rows = [dict(r) for r in cur.fetchall()]

    processed = []
    for row in raw_rows:
        # Pre-normalise score vector
        scores = np.array([row[c] for c in _SCORE_COLS], dtype=np.float32)
        snorm  = np.linalg.norm(scores)
        row["scores_norm"] = scores / snorm if snorm > 0 else None

        # Pre-normalise embedding vector (REAL[] → numpy)
        vec = row.get("embedding")
        if vec is not None:
            v = np.array(vec, dtype=np.float32)
            n = np.linalg.norm(v)
            row["emb_norm"] = v / n if n > 0 else None
        else:
            row["emb_norm"] = None

        # open_slots as a frozenset for fast Jaccard
        row["slots_set"] = frozenset(row.get("open_slots") or [])

        # SerpAPI tags as a frozenset for fast Jaccard
        row["tags_set"] = frozenset(row.get("tags") or [])

        # Cuisine / interior / food tags frozensets (new signal)
        row["cuisine_tags_set"]  = frozenset(row.get("cuisine_tags")  or [])
        row["interior_tags_set"] = frozenset(row.get("interior_tags") or [])
        row["food_tags_set"]     = frozenset(row.get("food_tags")     or [])

        # Critic sub-score vector (normalised) — None if all zeros (no critic data)
        critic_vec = np.array(
            [row["cuisine_score"], row["service_score"],
             row["value_score"],   row["ambiance_score"]],
            dtype=np.float32,
        )
        cnorm = np.linalg.norm(critic_vec)
        row["critic_norm"] = critic_vec / cnorm if cnorm > 0 else None

        processed.append(row)

    _CAND_CACHE[cache_key] = (_time.time(), processed)
    return processed


def _photo_url(pid: str, thumbnail_url: str | None) -> str | None:
    """Return filesystem URL if photo exists, else fall back to Serper thumbnail."""
    photo_dir = os.path.join(PHOTOS_DIR, pid)
    if os.path.isdir(photo_dir) and os.path.exists(os.path.join(photo_dir, "0.jpg")):
        return f"/static/photos/{pid}/0.jpg"
    return thumbnail_url or None


def _jaccard(a: frozenset, b: frozenset) -> float:
    u = a | b
    return len(a & b) / len(u) if u else 0.0


def _price_bonus(pl_a: str | None, pl_b: str | None) -> float:
    diff = abs(len(pl_a or "€") - len(pl_b or "€"))
    return 0.05 if diff == 0 else 0.025 if diff == 1 else 0.0


def _price_pp_bonus(
    pp_a: int | None, pp_b: int | None,
    pl_a: str | None = None, pl_b: str | None = None,
) -> float:
    """Continuous price similarity [0, 0.05].

    Uses avg_price_pp when both are available (more precise).
    Falls back to discrete price_level comparison otherwise.
    """
    if pp_a and pp_b:
        max_price = max(pp_a, pp_b, 1)
        return max(0.0, 0.05 * (1.0 - abs(pp_a - pp_b) / max_price))
    return _price_bonus(pl_a, pl_b)


def _compute_similarity(
    target: dict,
    candidates: list[dict],
    exclude_id: str,
    threshold: float = 0.50,
) -> list[tuple[float, str]]:
    """Unified similarity scorer — improved formula incorporating all signals.

    With embeddings (≈ sum 1.00 when all signals available):
      0.40 × embedding cosine
      0.20 × 11-dim profile-score cosine
      0.12 × cuisine_tags Jaccard      (food style match)
      0.10 × critic sub-score cosine   (0 when unavailable)
      0.08 × SerpAPI tag Jaccard
      0.05 × open-hours Jaccard
      + type / price / audience bonuses

    Without embeddings:
      0.50 × profile-score cosine
      0.20 × cuisine_tags Jaccard
      0.10 × critic sub-score cosine   (0 when unavailable)
      0.10 × SerpAPI tag Jaccard
      0.05 × open-hours Jaccard
      + type / price / audience bonuses
    """
    t_scores   = target["scores_norm"]
    t_emb      = target["emb_norm"]
    t_slots    = target["slots_set"]
    t_tags     = target["tags_set"]
    t_cuisine  = target["cuisine_tags_set"]
    t_critic   = target.get("critic_norm")
    t_type     = target["place_type"]
    t_price    = target["price_level"]
    t_pp       = target.get("avg_price_pp")
    t_audience = target.get("audience_type")
    use_emb    = t_emb is not None

    ranked: list[tuple[float, str]] = []
    for cand in candidates:
        if cand["place_id"] == exclude_id or cand["scores_norm"] is None:
            continue

        score_cos  = float(np.dot(t_scores, cand["scores_norm"]))
        c_emb      = cand["emb_norm"]
        emb_cos    = float(np.dot(t_emb, c_emb)) if use_emb and c_emb is not None else 0.0
        slots_j    = _jaccard(t_slots,   cand["slots_set"])
        tags_j     = _jaccard(t_tags,    cand["tags_set"])
        cuisine_j  = _jaccard(t_cuisine, cand["cuisine_tags_set"])
        c_critic   = cand.get("critic_norm")
        critic_cos = float(np.dot(t_critic, c_critic)) if (t_critic is not None and c_critic is not None) else 0.0
        type_b     = 0.05 if cand["place_type"] == t_type else 0.0
        price_b    = _price_pp_bonus(t_pp, cand.get("avg_price_pp"), t_price, cand.get("price_level"))
        audience_b = 0.03 if (t_audience and t_audience == cand.get("audience_type")) else 0.0

        if use_emb and c_emb is not None:
            sim = (0.40 * emb_cos
                 + 0.20 * score_cos
                 + 0.12 * cuisine_j
                 + 0.10 * critic_cos
                 + 0.08 * tags_j
                 + 0.05 * slots_j
                 + type_b + price_b + audience_b)
        else:
            sim = (0.50 * score_cos
                 + 0.20 * cuisine_j
                 + 0.10 * critic_cos
                 + 0.10 * tags_j
                 + 0.05 * slots_j
                 + type_b + price_b + audience_b)

        if sim >= threshold:
            ranked.append((sim, cand["place_id"]))

    ranked.sort(reverse=True)
    return ranked


# ── Recommender API ──────────────────────────────────────────────────────────

@app.route("/api/similar/<place_id>")
def api_similar(place_id):
    """Return up to n restaurants similar to <place_id>.

    Uses the improved _compute_similarity formula (cuisine tags, critic sub-scores,
    continuous price, audience bonus).  Only results ≥ 0.50 are returned.
    """
    n    = min(int(request.args.get("n", 6)), 20)
    conn = get_db()
    try:
        candidates = _load_candidates(conn)
    finally:
        conn.close()

    target = next((c for c in candidates if c["place_id"] == place_id), None)
    if not target or target["scores_norm"] is None:
        return jsonify([])

    ranked     = _compute_similarity(target, candidates, place_id, threshold=0.50)
    cand_by_id = {c["place_id"]: c for c in candidates}

    results = []
    for sim, pid in ranked[:n]:
        cand = cand_by_id[pid]
        type_emoji, _ = _classify_type(cand.get("place_type"))
        results.append({
            "place_id":    pid,
            "name":        cand["name"],
            "rating":      float(cand["rating"]) if cand["rating"] else None,
            "price_level": cand.get("price_level"),
            "type_emoji":  type_emoji,
            "vibe":        cand.get("vibe"),
            "photo_url":   _photo_url(pid, cand.get("thumbnail_url")),
            "similarity":  round(sim, 3),
        })
    return jsonify(results)


# ── Recommender v1 (pure SQL, no embeddings) ─────────────────────────────────
#
#  Kept for reference / A-B comparison.  Activate via:
#    GET /api/similar_v1/<place_id>?n=6
#
#  Composite similarity (0–1):
#    0.60 × cosine similarity of the 11 Gemini profile-score vectors  (SQL)
#    0.25 × Jaccard similarity of SerpAPI tag arrays                   (SQL)
#    0.10 × type bonus  (same place type)
#    0.05 × price bonus (same level = full, ±1 level = half)
#
#  No numpy, no extra tables — runs entirely inside PostgreSQL.
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/similar_v1/<place_id>")
def api_similar_v1(place_id):
    """V1 pure-SQL recommender — no embeddings required.

    Composite similarity:
      0.60 × cosine similarity of 11 Gemini profile scores
      0.25 × Jaccard similarity of SerpAPI tag arrays
      0.10 × type bonus
      0.05 × price bonus

    Only results with similarity ≥ 0.50 are returned.
    """
    n    = min(int(request.args.get("n", 6)), 20)
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH target AS (
                    SELECT
                        COALESCE(e.family_score,    0) AS family_score,
                        COALESCE(e.date_score,      0) AS date_score,
                        COALESCE(e.friends_score,   0) AS friends_score,
                        COALESCE(e.solo_score,      0) AS solo_score,
                        COALESCE(e.relaxed_score,   0) AS relaxed_score,
                        COALESCE(e.party_score,     0) AS party_score,
                        COALESCE(e.special_score,   0) AS special_score,
                        COALESCE(e.foodie_score,    0) AS foodie_score,
                        COALESCE(e.lingering_score, 0) AS lingering_score,
                        COALESCE(e.unique_score,    0) AS unique_score,
                        COALESCE(e.dresscode_score, 0) AS dresscode_score,
                        sd.atmosphere || COALESCE(sd.offerings, '{}') ||
                        COALESCE(sd.crowd,      '{}') || COALESCE(sd.highlights,'{}') AS tags,
                        rs.raw_data->>'type' AS place_type,
                        tr.price_level
                    FROM  top_restaurants tr
                    LEFT JOIN gemini_enrichments e  ON e.place_id  = tr.place_id
                    LEFT JOIN serpapi_details    sd ON sd.place_id = tr.place_id
                    LEFT JOIN restaurants       rs  ON rs.place_id = tr.place_id
                    WHERE tr.place_id = %(place_id)s
                )
                SELECT *
                FROM (
                    SELECT
                        r.place_id,
                        r.name,
                        r.rating,
                        r.price_level,
                        r.thumbnail_url,
                        e.vibe,
                        res2.raw_data->>'type' AS place_type,
                        -- 60pct: cosine similarity of 11 profile-score vectors
                        (
                            COALESCE(e.family_score,0)    * t.family_score
                          + COALESCE(e.date_score,0)      * t.date_score
                          + COALESCE(e.friends_score,0)   * t.friends_score
                          + COALESCE(e.solo_score,0)      * t.solo_score
                          + COALESCE(e.relaxed_score,0)   * t.relaxed_score
                          + COALESCE(e.party_score,0)     * t.party_score
                          + COALESCE(e.special_score,0)   * t.special_score
                          + COALESCE(e.foodie_score,0)    * t.foodie_score
                          + COALESCE(e.lingering_score,0) * t.lingering_score
                          + COALESCE(e.unique_score,0)    * t.unique_score
                          + COALESCE(e.dresscode_score,0) * t.dresscode_score
                        )::float / NULLIF(
                            SQRT(
                                COALESCE(e.family_score,0)^2    + COALESCE(e.date_score,0)^2
                              + COALESCE(e.friends_score,0)^2   + COALESCE(e.solo_score,0)^2
                              + COALESCE(e.relaxed_score,0)^2   + COALESCE(e.party_score,0)^2
                              + COALESCE(e.special_score,0)^2   + COALESCE(e.foodie_score,0)^2
                              + COALESCE(e.lingering_score,0)^2 + COALESCE(e.unique_score,0)^2
                              + COALESCE(e.dresscode_score,0)^2
                            ) * SQRT(
                                t.family_score^2    + t.date_score^2
                              + t.friends_score^2   + t.solo_score^2
                              + t.relaxed_score^2   + t.party_score^2
                              + t.special_score^2   + t.foodie_score^2
                              + t.lingering_score^2 + t.unique_score^2
                              + t.dresscode_score^2
                            )
                        , 0) * 0.60
                        -- 25pct: Jaccard similarity of SerpAPI tag arrays
                        + COALESCE(
                            CARDINALITY(ARRAY(
                                SELECT unnest(
                                    COALESCE(sd.atmosphere,'{}') || COALESCE(sd.offerings,'{}') ||
                                    COALESCE(sd.crowd,     '{}') || COALESCE(sd.highlights,'{}')
                                )
                                INTERSECT
                                SELECT unnest(t.tags)
                            ))::float /
                            NULLIF(CARDINALITY(ARRAY(
                                SELECT unnest(
                                    COALESCE(sd.atmosphere,'{}') || COALESCE(sd.offerings,'{}') ||
                                    COALESCE(sd.crowd,     '{}') || COALESCE(sd.highlights,'{}')
                                )
                                UNION
                                SELECT unnest(t.tags)
                            )), 0)
                        , 0) * 0.25
                        -- 10pct: type bonus
                        + CASE WHEN res2.raw_data->>'type' = t.place_type THEN 0.10 ELSE 0 END
                        -- 5pct: price level bonus
                        + CASE
                            WHEN r.price_level = t.price_level THEN 0.05
                            WHEN ABS(
                                LENGTH(COALESCE(r.price_level, '€')) -
                                LENGTH(COALESCE(t.price_level, '€'))
                            ) = 1 THEN 0.025
                            ELSE 0
                          END
                        AS similarity
                    FROM  top_restaurants      r
                    JOIN  gemini_enrichments   e    ON e.place_id    = r.place_id
                    LEFT JOIN serpapi_details  sd   ON sd.place_id   = r.place_id
                    LEFT JOIN restaurants      res2 ON res2.place_id = r.place_id
                    CROSS JOIN target t
                    WHERE r.place_id != %(place_id)s
                ) sub
                WHERE  similarity >= 0.50
                ORDER  BY similarity DESC
                LIMIT  %(n)s
            """, {"place_id": place_id, "n": n})
            rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for row in rows:
        pid = row["place_id"]
        type_emoji, _ = _classify_type(row.get("place_type"))
        results.append({
            "place_id":    pid,
            "name":        row["name"],
            "rating":      float(row["rating"]) if row["rating"] else None,
            "price_level": row.get("price_level"),
            "type_emoji":  type_emoji,
            "vibe":        row.get("vibe"),
            "photo_url":   _photo_url(pid, row.get("thumbnail_url")),
            "similarity":  round(float(row["similarity"]), 3),
        })
    return jsonify(results)


# ── Recommender v3 (embeddings + tags Jaccard, lower emb weight) ─────────────
#
#  Like v2 but adds SerpAPI tag Jaccard and reduces embedding weight:
#    0.40 × embedding cosine   (↓ from 0.55 in v2)
#    0.20 × profile-score cosine
#    0.15 × SerpAPI tag Jaccard  (new vs v2)
#    0.15 × open-hours Jaccard
#    0.05 × type bonus
#    0.05 × price bonus
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/similar_v3/<place_id>")
def api_similar_v3(place_id):
    """V3 recommender — same improved formula as v2 (alias for A/B testing)."""
    n    = min(int(request.args.get("n", 6)), 20)
    conn = get_db()
    try:
        candidates = _load_candidates(conn)
    finally:
        conn.close()

    target = next((c for c in candidates if c["place_id"] == place_id), None)
    if not target or target["scores_norm"] is None:
        return jsonify([])

    ranked     = _compute_similarity(target, candidates, place_id, threshold=0.50)
    cand_by_id = {c["place_id"]: c for c in candidates}

    results = []
    for sim, pid in ranked[:n]:
        cand = cand_by_id[pid]
        type_emoji, _ = _classify_type(cand.get("place_type"))
        results.append({
            "place_id":    pid,
            "name":        cand["name"],
            "rating":      float(cand["rating"]) if cand["rating"] else None,
            "price_level": cand.get("price_level"),
            "type_emoji":  type_emoji,
            "vibe":        cand.get("vibe"),
            "photo_url":   _photo_url(pid, cand.get("thumbnail_url")),
            "similarity":  round(sim, 3),
        })
    return jsonify(results)


# ── Shared helper ────────────────────────────────────────────────────────────

def _enrich_row(r: dict) -> dict:
    """Attach computed fields (city, type_emoji, photos) to a restaurant row dict."""
    r["city"]        = _extract_city(r.get("address"))
    r["type_emoji"], r["type_label"] = _classify_type(r.get("place_type"))
    photo_dir = os.path.join(PHOTOS_DIR, r["place_id"])
    photos: list[str] = []
    if os.path.isdir(photo_dir):
        # SerpAPI photos: 0.jpg, 1.jpg, …
        for i in range(MAX_PHOTOS):
            path = os.path.join(photo_dir, f"{i}.jpg")
            if os.path.exists(path):
                photos.append(f"/static/photos/{r['place_id']}/{i}.jpg")
            else:
                break
        # Website scraper photos: 0_websiteScraper.jpg, 1_websiteScraper.jpg, …
        ws_files = sorted(
            f for f in os.listdir(photo_dir) if f.endswith("_websiteScraper.jpg")
        )
        for f in ws_files:
            photos.append(f"/static/photos/{r['place_id']}/{f}")
    if not photos and r.get("thumbnail_url"):
        photos = [r["thumbnail_url"]]
    r["photos"] = photos
    return r


# ── Context processor ─────────────────────────────────────────────────────────

@app.context_processor
def inject_city_helpers():
    """Inject city_url() helper and all_cities list into all templates."""
    def city_url(endpoint: str, **kwargs) -> str:
        city = g.get("current_city")
        if city:
            return url_for(endpoint, city=city["slug"], **kwargs)
        return url_for(endpoint, **kwargs)

    try:
        conn = get_db()
        try:
            all_cities = list(_load_cities(conn).values())
        finally:
            conn.close()
    except Exception:
        all_cities = []

    return {"city_url": city_url, "all_cities": all_cities}


# ── Landing page ──────────────────────────────────────────────────────────────

@app.route("/")
def landing():
    """EatEatEat landing page — city selector."""
    conn = get_db()
    try:
        cities = list(_load_cities(conn).values())
    finally:
        conn.close()
    return render_template("landing.html", cities=cities)


# ── 301 redirects for old URLs ────────────────────────────────────────────────

@app.route("/restaurant/<place_id>")
def restaurant_redirect(place_id):
    return redirect(f"/mallorca/restaurant/{place_id}", code=301)

@app.route("/similar/<place_id>")
def similar_redirect(place_id):
    return redirect(f"/mallorca/similar/{place_id}", code=301)

@app.route("/listen")
def listen_redirect():
    return redirect("/mallorca/listen", code=301)

@app.route("/listen/<slug>")
def listen_collection_redirect(slug):
    return redirect(f"/mallorca/listen/{slug}", code=301)


@app.route("/tipps/<slug>")
def tipps_article_redirect(slug):
    return redirect(f"/mallorca/tipps/{slug}", code=301)

@app.route("/discover")
def discover_redirect():
    return redirect("/mallorca/discover", code=301)


# ── Restaurant detail page ────────────────────────────────────────────────────

@app.route("/<city>/restaurant/random")
def restaurant_random(city):
    """Redirect to a random enriched restaurant."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.place_id
                FROM top_restaurants t
                JOIN gemini_enrichments e ON e.place_id = t.place_id
                WHERE e.family_score IS NOT NULL
                  AND t.city_id = %(city_id)s
                ORDER BY RANDOM()
                LIMIT 1
            """, {"city_id": city_data["id"]})
            row = cur.fetchone()
    finally:
        conn.close()
    return redirect(f"/{city}/restaurant/{row[0]}") if row else redirect(f"/{city}/")


@app.route("/<city>/restaurant/<place_id>")
def restaurant_page(city, place_id):
    """Full detail page for a single restaurant."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    sid = request.cookies.get("sid") or ""

    conn = get_db()
    try:
        # ── Main restaurant data ──────────────────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    t.*,
                    e.family_score,  e.date_score,    e.friends_score, e.solo_score,
                    e.relaxed_score, e.party_score,   e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.dresscode_score,
                    e.outdoor_score, e.view_score,
                    e.cuisine_score, e.service_score, e.value_score,
                    e.ambiance_score, e.critic_score,
                    e.audience_type, e.avg_price_pp,
                    e.cuisine_type,  e.cuisine_tags,
                    e.interior_tags, e.food_tags,
                    e.summary_de, e.must_order, e.vibe,
                    sd.highlights, sd.popular_for, sd.offerings, sd.atmosphere,
                    sd.crowd, sd.planning, sd.amenities, sd.dining_options,
                    sd.service_options,
                    res.raw_data->>'type' AS place_type,
                    res.phone,
                    f.list_type AS fav_type,
                    f.score     AS fav_score
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                LEFT JOIN serpapi_details    sd ON sd.place_id = t.place_id
                LEFT JOIN restaurants       res ON res.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                               AND f.session_id = %(sid)s
                WHERE t.place_id = %(pid)s
                  AND t.city_id = %(city_id)s
            """, {"pid": place_id, "sid": sid, "city_id": city_data["id"]})
            row = cur.fetchone()

        if not row:
            return render_template("restaurant.html", r=None, similar=[], prev_id=None, next_id=None, city=city_data, error="Restaurant nicht gefunden.")

        # ── Prev / Next navigation (ordered by rating) ────────────────────
        with conn.cursor() as cur:
            cur.execute("""
                WITH ordered AS (
                    SELECT
                        t.place_id,
                        LAG(t.place_id)  OVER (ORDER BY t.rating DESC, t.rating_count DESC, t.place_id) AS prev_id,
                        LEAD(t.place_id) OVER (ORDER BY t.rating DESC, t.rating_count DESC, t.place_id) AS next_id
                    FROM top_restaurants t
                    JOIN gemini_enrichments e ON e.place_id = t.place_id
                    WHERE e.family_score IS NOT NULL
                      AND t.city_id = %(city_id)s
                )
                SELECT prev_id, next_id FROM ordered WHERE place_id = %(pid)s
            """, {"pid": place_id, "city_id": city_data["id"]})
            nav = cur.fetchone()
        prev_id = nav[0] if nav else None
        next_id = nav[1] if nav else None

        # ── Similar restaurants (improved formula, top 6) ─────────────────
        candidates  = _load_candidates(conn, city_id=city_data["id"])
        target_cand = next((c for c in candidates if c["place_id"] == place_id), None)
        similar_ids: list[str] = []
        sim_by_id:   dict[str, float] = {}

        if target_cand and target_cand["scores_norm"] is not None:
            ranked      = _compute_similarity(target_cand, candidates, place_id, threshold=0.45)
            similar_ids = [pid for _, pid in ranked[:12]]
            sim_by_id   = {pid: sim for sim, pid in ranked[:12]}

        # Fetch similar restaurant details
        similar_rows: list[dict] = []
        if similar_ids:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT t.*, e.family_score, e.date_score, e.friends_score, e.solo_score,
                           e.relaxed_score, e.special_score, e.foodie_score,
                           e.outdoor_score, e.view_score, e.avg_price_pp,
                           e.cuisine_type, e.vibe, e.critic_score,
                           res.raw_data->>'type' AS place_type,
                           f.list_type AS fav_type, f.score AS fav_score
                    FROM top_restaurants t
                    LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                    LEFT JOIN restaurants       res ON res.place_id = t.place_id
                    LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                                   AND f.session_id = %(sid)s
                    WHERE t.place_id = ANY(%(ids)s)
                """, {"ids": similar_ids, "sid": sid})
                sim_raw = {r["place_id"]: dict(r) for r in cur.fetchall()}
            for pid in similar_ids:
                if pid in sim_raw:
                    sr = _enrich_row(sim_raw[pid])
                    sr["similarity"] = round(sim_by_id[pid], 3)
                    similar_rows.append(sr)

        # ── Editorial article (if exists) ─────────────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT slug, title, article_md
                FROM editorial_articles
                WHERE place_id = %s AND is_published = TRUE
            """, (place_id,))
            article_row = cur.fetchone()

        article = None
        if article_row:
            article = dict(article_row)
            article["preview_html"] = _md_to_html(article["article_md"], preview_only=True)

    finally:
        conn.close()

    r = _enrich_row(dict(row))
    resp = make_response(render_template("restaurant.html",
                                         r=r,
                                         similar=similar_rows,
                                         prev_id=prev_id,
                                         next_id=next_id,
                                         profiles=PROFILES,
                                         article=article,
                                         city=city_data,
                                         error=None))
    if not request.cookies.get("sid"):
        resp.set_cookie("sid", str(uuid.uuid4()), max_age=60*60*24*365, httponly=True, samesite="Lax")
    return resp


# ── Similar restaurants — full page view ─────────────────────────────────────

@app.route("/<city>/similar/<place_id>")
def similar_page(city, place_id):
    """Full-page view: target restaurant + all similar restaurants (≥ 0.75)."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    sid  = request.cookies.get("sid") or ""

    conn = get_db()
    try:
        candidates = _load_candidates(conn, city_id=city_data["id"])

        # Score using improved formula
        target = next((c for c in candidates if c["place_id"] == place_id), None)
        if not target or target["scores_norm"] is None:
            return render_template("similar.html", target=None, restaurants=[], city=city_data, error="Restaurant nicht gefunden.")

        ranked      = _compute_similarity(target, candidates, place_id, threshold=0.55)
        similar_ids = [pid for _, pid in ranked]
        sim_by_id   = {pid: sim for sim, pid in ranked}
        fetch_ids   = [place_id] + similar_ids

        # Fetch full restaurant data for target + similar
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    t.*,
                    e.family_score,  e.date_score,    e.friends_score, e.solo_score,
                    e.relaxed_score, e.party_score,   e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.dresscode_score,
                    e.outdoor_score, e.view_score,
                    e.cuisine_score, e.service_score, e.value_score,
                    e.ambiance_score, e.critic_score,
                    e.audience_type, e.avg_price_pp,
                    e.cuisine_type,  e.cuisine_tags,
                    e.summary_de, e.must_order, e.vibe,
                    sd.highlights, sd.popular_for, sd.offerings, sd.atmosphere,
                    sd.crowd, sd.planning, sd.amenities, sd.dining_options,
                    sd.service_options,
                    res.raw_data->>'type' AS place_type,
                    f.list_type  AS fav_type,
                    f.score      AS fav_score
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                LEFT JOIN serpapi_details    sd ON sd.place_id = t.place_id
                LEFT JOIN restaurants       res ON res.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                               AND f.session_id = %(sid)s
                WHERE t.place_id = ANY(%(ids)s)
            """, {"ids": fetch_ids, "sid": sid})
            rows = {row["place_id"]: dict(row) for row in cur.fetchall()}

    finally:
        conn.close()

    target_row = _enrich_row(rows[place_id]) if place_id in rows else None
    similar_rows = []
    for pid in similar_ids:
        if pid in rows:
            row = _enrich_row(rows[pid])
            row["similarity"] = round(sim_by_id[pid], 3)
            similar_rows.append(row)

    return render_template("similar.html",
                           target=target_row,
                           restaurants=similar_rows,
                           profiles=PROFILES,
                           city=city_data,
                           error=None)


# ── Discover v2 — concrete filter funnel ─────────────────────────────────────


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


_DISCOVER_CUISINES = [
    {"key": "mediterran",    "label": "Mediterran",          "emoji": "🌊",
     "types": {"Modern-Mediterran","Mediterran","Mallorquinisch",
               "Mallorquinisch-Mediterran","Modern-Mallorquinisch",
               "Mediterran & International","Mediterrane Bar"}},
    {"key": "cafe",          "label": "Café & Brunch",       "emoji": "☕",
     "types": {"Café & Brunch","Mallorquinische Bäckerei"}},
    {"key": "tapas",         "label": "Tapas & Spanisch",    "emoji": "🫒",
     "types": {"Tapas-Bar","Spanische Tapas","Spanische Bar"}},
    {"key": "italian",       "label": "Italienisch & Pizza", "emoji": "🍕",
     "types": {"Italienisch","Italienisch, Pizza","Neapolitanische Pizza","Pizzeria"}},
    {"key": "international", "label": "International",       "emoji": "🌍",
     "types": {"Indisch","Japanisch","Modern-International","Asiatisch","Sushi",
               "Mexikanisch","Chinesisch","Griechisch","Arabisch","Türkisch",
               "Libanesisch","Modern International","Modern-European"}},
    {"key": "weinbar",       "label": "Weinbar",             "emoji": "🍷",
     "types": {"Weinbar","Weinbar & Tapas","Weinbar & Mallorquinisch",
               "Weinbar & Regionale Küche","Weinbar & Snacks","Tapas & Weinbar",
               "Modern-Mediterran & Weinbar","Weinbar & Cocktailbar","Weinbar & Weingut",
               "Weinbar mit Events","Weinbar & Delikatessen","Mallorquinische Weinbar",
               "Modern-Mediterran, Weinbar","Weinbar & Mediterrane Küche","Weinbar & Café",
               "Bodega & Weinbar","Weinrestaurant & Modern-Mediterran",
               "Mallorquinisch & Wein","Weingut & Weinprobe","Kleine Teller & Weinbar",
               "Weinbar, Mallorquinisch","Weinprobe & Mallorquinisch",
               "Weingut & Tapas","Weinhandlung & Tasting"}},
    {"key": "bar",           "label": "Bar & Cocktails",     "emoji": "🍸",
     "types": {"Cocktailbar","Cocktailbar & Snacks","Café & Bar","Bar & Snacks",
               "Internationale Bar-Küche","Internationale Bar","Tapas & Cocktails",
               "Mallorquinische Bar","Craft Beer Bar","Italienische Bar","Bar & Cocktails",
               "Cocktailbar & Tapas","Gourmet-Bar","Cocktailbar & Barfood",
               "Mediterran & Strandbar","Lounge & Bar Snacks","Bar",
               "Cocktailbar & Lounge","Bar & Grill","Jazzbar & Snacks",
               "Klassische Cocktailbar","Bar & Mediterran","Cocktailbar & Aperitivo",
               "Bar & Café","Spanische Bar","Mediterrane Bar","Mediterrane Bar-Küche",
               "Mediterrane Strandbar","Bar & Tapas","Tapas-Bar & Cocktails",
               "Modern-Mediterran & Cocktails"}},
]

_DISCOVER_QUESTIONS = [
    {
        "key":      "location",
        "subtype":  "location_picker",
        "emoji":    "📍",
        "question": "Wo auf Mallorca bist du?",
        "options":  [],  # rendered specially in the frontend
    },
    {
        "key":      "cuisine",
        "emoji":    "🍽",
        "question": "Was willst du essen?",
        "options": [
            {"value": "mediterran",    "label": "Mediterran",          "emoji": "🌊"},
            {"value": "cafe",          "label": "Café & Brunch",       "emoji": "☕"},
            {"value": "tapas",         "label": "Tapas & Spanisch",    "emoji": "🫒"},
            {"value": "italian",       "label": "Italienisch & Pizza", "emoji": "🍕"},
            {"value": "international", "label": "International",       "emoji": "🌍"},
            {"value": "weinbar",       "label": "Weinbar",             "emoji": "🍷"},
            {"value": "bar",           "label": "Bar & Cocktails",     "emoji": "🍸"},
            {"value": "egal",          "label": "Egal",                "emoji": "🎲"},
        ],
    },
    {
        "key":      "budget",
        "emoji":    "💰",
        "question": "Was darf es kosten?",
        "options": [
            {"value": "cheap",  "label": "Günstig",  "emoji": "🪙", "desc": "bis 20 € p.P."},
            {"value": "mid",    "label": "Mittel",   "emoji": "💳", "desc": "20–40 € p.P."},
            {"value": "high",   "label": "Gehoben",  "emoji": "🥂", "desc": "40 € + p.P."},
            {"value": "egal",   "label": "Egal",     "emoji": "🎲"},
        ],
    },
    {
        "key":      "outdoor",
        "emoji":    "🌿",
        "question": "Draußen sitzen oder Meerblick?",
        "options": [
            {"value": "yes",    "label": "Ja, unbedingt",   "emoji": "🌞"},
            {"value": "indoor", "label": "Drinnen ist ok",  "emoji": "🏠"},
            {"value": "egal",   "label": "Egal",            "emoji": "🎲"},
        ],
    },
]


def _discover_city(address: str) -> str | None:
    """Extract city name from '07xxx CityName, ...' address format."""
    import re
    m = re.search(r"07\d{3}\s+([^,]+)", address or "")
    return m.group(1).strip() if m else None


def _discover_pool_size(conn, filters: dict) -> int:
    """Count restaurants matching the current set of filters."""
    all_rows = _discover_fetch_all(conn)
    return len(_discover_filter(all_rows, filters))


_DISCOVER_CACHE: dict[int, tuple] = {}  # key: city_id
_DISCOVER_CACHE_TTL = 300


def _discover_fetch_all(conn, city_id: int | None = None) -> list[dict]:
    global _DISCOVER_CACHE
    import time
    now = time.time()
    cache_key = city_id if city_id is not None else -1
    if cache_key in _DISCOVER_CACHE and now - _DISCOVER_CACHE[cache_key][0] < _DISCOVER_CACHE_TTL:
        return _DISCOVER_CACHE[cache_key][1]

    city_filter = "AND r.city_id = %(city_id)s" if city_id is not None else ""
    city_params = {"city_id": city_id} if city_id is not None else {}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT r.place_id, r.name, r.address, r.rating, r.rating_count,
                   r.thumbnail_url, r.price_level, r.latitude, r.longitude,
                   ge.avg_price_pp, ge.cuisine_type, ge.cuisine_tags,
                   ge.interior_tags, ge.vibe, ge.outdoor_score, ge.view_score
            FROM restaurants r
            JOIN gemini_enrichments ge ON ge.place_id = r.place_id
            WHERE r.is_active = TRUE {city_filter}
            ORDER BY r.rating DESC, r.rating_count DESC NULLS LAST
        """, city_params)
        rows = [dict(r) for r in cur.fetchall()]

    for r in rows:
        r["city"] = _discover_city(r.get("address") or "")
        # Cast to float so Haversine works even with Decimal types
        if r.get("latitude"):  r["latitude"]  = float(r["latitude"])
        if r.get("longitude"): r["longitude"] = float(r["longitude"])

    _DISCOVER_CACHE[cache_key] = (now, rows)
    return rows


def _discover_filter(rows: list[dict], filters: dict) -> list[dict]:
    result = rows

    # Location — coordinate-based radius filter
    loc = filters.get("location")
    if loc and isinstance(loc, dict):
        lat      = loc.get("lat")
        lon      = loc.get("lon")
        radius   = loc.get("radius_km", 10)
        if lat is not None and lon is not None:
            result = [
                r for r in result
                if r.get("latitude") and r.get("longitude")
                and _haversine_km(lat, lon, r["latitude"], r["longitude"]) <= radius
            ]

    # Cuisine
    cuisine = filters.get("cuisine")
    if cuisine and cuisine != "egal":
        cuisine_types = next(
            (c["types"] for c in _DISCOVER_CUISINES if c["key"] == cuisine), set()
        )
        result = [r for r in result if r.get("cuisine_type") in cuisine_types]

    # Budget
    budget = filters.get("budget")
    if budget and budget != "egal":
        def _price_ok(r):
            pp = r.get("avg_price_pp")
            if pp is None:
                return True  # include unknowns
            if budget == "cheap":  return pp < 20
            if budget == "mid":    return 20 <= pp <= 40
            if budget == "high":   return pp > 40
            return True
        result = [r for r in result if _price_ok(r)]

    # Outdoor / Meerblick
    outdoor = filters.get("outdoor")
    if outdoor == "yes":
        result = [r for r in result
                  if (r.get("outdoor_score") or 0) >= 7
                  or (r.get("view_score") or 0) >= 7]
    elif outdoor == "indoor":
        result = [r for r in result
                  if (r.get("outdoor_score") or 5) <= 5
                  and (r.get("view_score") or 0) < 8]

    return result


def _discover_question_card(card_index: int, filters: dict, pool_size: int) -> dict:
    q = _DISCOVER_QUESTIONS[card_index]
    return {
        "type":       "question",
        "subtype":    q.get("subtype", "options"),
        "card_index": card_index,
        "total":      len(_DISCOVER_QUESTIONS),
        "key":        q["key"],
        "emoji":      q["emoji"],
        "question":   q["question"],
        "options":    q["options"],
        "pool_size":  pool_size,
    }


def _discover_result_card(rows: list[dict]) -> dict:
    results = []
    for r in rows[:20]:
        tags = (r.get("cuisine_tags") or [])[:3] + (r.get("interior_tags") or [])[:2]
        photo = _photo_url(r["place_id"], r.get("thumbnail_url"))
        results.append({
            "place_id":     r["place_id"],
            "name":         r["name"],
            "city":         r.get("city") or "",
            "rating":       float(r.get("rating") or 0),
            "rating_count": r.get("rating_count") or 0,
            "price_pp":     r.get("avg_price_pp"),
            "cuisine_type": r.get("cuisine_type") or "",
            "tags":         tags,
            "vibe":         (r.get("vibe") or "")[:120],
            "photo_url":    photo,
        })
    return {"type": "results", "restaurants": results, "total": len(rows)}


@app.route("/<city>/api/geocode")
def api_geocode(city):
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "no query"}), 400
    headers = {"User-Agent": "MallorcaEat/1.0", "Accept-Language": "de,en"}

    def _nom_get(url):
        r = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(r, timeout=5) as resp:
            return json.loads(resp.read())

    try:
        # 1st try: structured city search — returns the actual settlement
        city_q = urllib.parse.urlencode({"city": q, "countrycodes": "es",
                                         "format": "json", "limit": 5,
                                         "addressdetails": 1})
        results = _nom_get(f"https://nominatim.openstreetmap.org/search?{city_q}")

        # Filter to Balearic Islands results
        baleares = [r for r in results
                    if "Illes Balears" in r.get("display_name", "")
                    or "Balear" in r.get("display_name", "")]
        if not baleares:
            baleares = results  # fallback: take whatever we get

        if not baleares:
            # 2nd try: free-text with Mallorca context
            fq = urllib.parse.urlencode({"q": f"{q}, Mallorca", "countrycodes": "es",
                                         "format": "json", "limit": 10})
            all_results = _nom_get(f"https://nominatim.openstreetmap.org/search?{fq}")
            place_types = {"city", "town", "village", "hamlet", "suburb", "municipality"}
            baleares = [r for r in all_results if r.get("type") in place_types]
            if not baleares:
                baleares = all_results

        if not baleares:
            return jsonify({"error": "not found"}), 404

        r = baleares[0]
        name = r.get("display_name", "").split(",")[0].strip()
        return jsonify({"lat": float(r["lat"]), "lon": float(r["lon"]), "name": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/<city>/discover")
def discover(city):
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    return render_template("discover.html", city=city_data)


@app.route("/<city>/api/discover/start", methods=["POST"])
def api_discover_start(city):
    city_data = _get_city_or_404(city)
    conn = get_db()
    total = len(_discover_fetch_all(conn, city_id=city_data["id"]))
    conn.close()
    session["discover_filters"] = {}
    session["discover_city_id"] = city_data["id"]
    session.modified = True
    return jsonify(_discover_question_card(0, {}, total))


@app.route("/<city>/api/discover/answer", methods=["POST"])
def api_discover_answer(city):
    city_data = _get_city_or_404(city)
    city_id = city_data["id"]
    req     = request.json or {}
    filters = session.get("discover_filters", {})
    key     = req.get("key")
    answer  = req.get("answer", "egal")

    if key and answer != "egal":
        if key == "location" and req.get("lat") is not None:
            filters["location"] = {
                "lat":       req["lat"],
                "lon":       req["lon"],
                "radius_km": req.get("radius_km", 10),
                "name":      req.get("location_name", ""),
            }
        else:
            filters[key] = answer

    session["discover_filters"] = filters
    session.modified = True

    conn  = get_db()
    rows  = _discover_fetch_all(conn, city_id=city_id)
    next_index = req.get("next_index", 0)

    if next_index >= len(_DISCOVER_QUESTIONS):
        filtered = _discover_filter(rows, filters)
        conn.close()
        return jsonify(_discover_result_card(filtered))

    pool_size = len(_discover_filter(rows, filters))
    conn.close()
    return jsonify(_discover_question_card(next_index, filters, pool_size))


# ── Main view ────────────────────────────────────────────────────────────────

@app.route("/<city>/")
def index(city):
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()

    # Session cookie (anonymous, persistent)
    sid     = request.cookies.get("sid") or ""
    new_sid = not sid
    if new_sid:
        sid = str(uuid.uuid4())

    search         = request.args.get("search", "").strip()
    location       = request.args.get("location", "").strip()   # legacy city-name filter
    loc_lat        = request.args.get("lat", "").strip()
    loc_lon        = request.args.get("lon", "").strip()
    loc_radius_km  = request.args.get("radius_km", "10").strip()
    location_name  = request.args.get("location_name", "").strip()
    profile_key    = request.args.get("profile", "").strip()
    min_score      = int(request.args.get("min_score", 7))
    page           = max(1, int(request.args.get("page", 1) or 1))
    view           = request.args.get("view", "").strip()           # "" | "want" | "been"
    sort           = request.args.get("sort", "").strip()           # "" | "quality"
    cuisine_filter = request.args.get("cuisine_filter", "").strip()
    price_filter   = request.args.get("price_filter",   "").strip()   # "low" | "mid" | "high" | "vhigh"

    _args_no_cuisine = {k: v for k, v in request.args.items() if k != "cuisine_filter"}
    _args_no_price   = {k: v for k, v in request.args.items() if k != "price_filter"}

    ctx = {
        "total": 0, "top_count": 0, "avg_rating": "–", "enriched_count": 0,
        "restaurants": [], "locations": [],
        "search": search, "location": location,
        "loc_lat": loc_lat, "loc_lon": loc_lon,
        "loc_radius_km": loc_radius_km, "location_name": location_name,
        "profile_key": profile_key, "min_score": min_score,
        "profiles": PROFILES,
        "page": page, "total_pages": 1, "total_filtered": 0, "per_page": PER_PAGE,
        "view": view, "sort": sort,
        "cuisine_filter": cuisine_filter,
        "qs_no_cuisine": urlencode(_args_no_cuisine),
        "price_filter": price_filter,
        "qs_no_price":  urlencode(_args_no_price),
        "top_cuisines": [],        # list[tuple[str, int]]
        "cuisine_neighbors": {},   # dict[str, list[str]]
        "city": city_data,
        "error": None,
    }

    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("SELECT COUNT(*) AS n FROM restaurants")
            ctx["total"] = cur.fetchone()["n"]

            cur.execute("SELECT ROUND(AVG(rating)::numeric,1) AS a FROM restaurants WHERE rating IS NOT NULL")
            r = cur.fetchone(); ctx["avg_rating"] = str(r["a"]) if r["a"] else "–"

            cur.execute("SELECT COUNT(*) AS n FROM top_restaurants")
            ctx["top_count"] = cur.fetchone()["n"]

            cur.execute("SELECT COUNT(*) AS n FROM gemini_enrichments")
            ctx["enriched_count"] = cur.fetchone()["n"]

            cur.execute("SELECT DISTINCT location FROM serper_cache ORDER BY location")
            ctx["locations"] = [r["location"] for r in cur.fetchall()]

            # Cuisine filter: load neighbors + top cuisine_types
            cuisine_neighbors        = _load_cuisine_neighbors(conn)
            ctx["cuisine_neighbors"] = cuisine_neighbors
            ctx["top_cuisines"]      = _load_top_cuisines(conn, city_id=city_data["id"] if city_data else None)

            # Build main query
            score_col  = f"{profile_key}_score" if profile_key else None
            conditions = ["1=1"]
            params: dict = {"sid": sid}

            # City filter
            conditions.append("t.city_id = %(city_id)s")
            params["city_id"] = city_data["id"]

            semantic_order = False   # flag: sort by semantic score instead of curation
            if search:
                # Semantic search via Jina embeddings (falls back to ILIKE if no embeddings yet)
                MIN_SEMANTIC_SCORE = 0.25   # below this → not relevant enough to show
                embed_result = _load_search_embeddings(conn, city_data["id"])
                if embed_result:
                    try:
                        model = _get_jina_model()
                        q_vec = model.encode(
                            [search],
                            task="retrieval.query",
                            normalize_embeddings=True,
                        )[0]
                        place_ids_all, matrix = embed_result
                        scores = matrix @ q_vec          # cosine similarity (N,)
                        # Only keep results above threshold, sorted by score desc
                        top_idxs = np.argsort(scores)[::-1]
                        semantic_ids = [
                            place_ids_all[i] for i in top_idxs
                            if scores[i] >= MIN_SEMANTIC_SCORE
                        ]
                        # semantic_ids is already in score order — use array_position to
                        # preserve that order in SQL
                        conditions.append("t.place_id = ANY(%(semantic_ids)s)")
                        params["semantic_ids"] = semantic_ids or ["__no_match__"]
                        semantic_order = bool(semantic_ids)
                    except Exception:
                        # Fallback to ILIKE on any model error
                        conditions.append("""(
                            t.name         ILIKE %(search)s OR
                            e.cuisine_type ILIKE %(search)s OR
                            e.summary_de   ILIKE %(search)s
                        )""")
                        params["search"] = f"%{search}%"
                else:
                    # No Jina embeddings available yet → ILIKE fallback
                    conditions.append("""(
                        t.name         ILIKE %(search)s OR
                        e.cuisine_type ILIKE %(search)s OR
                        e.summary_de   ILIKE %(search)s
                    )""")
                    params["search"] = f"%{search}%"
            if loc_lat and loc_lon:
                try:
                    _lat = float(loc_lat); _lon = float(loc_lon)
                    _rad = float(loc_radius_km) if loc_radius_km else 10.0
                    cur.execute(
                        "SELECT place_id, latitude, longitude FROM restaurants "
                        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
                    )
                    nearby_ids = [
                        r["place_id"] for r in cur.fetchall()
                        if _haversine_km(_lat, _lon, float(r["latitude"]), float(r["longitude"])) <= _rad
                    ]
                    conditions.append("t.place_id = ANY(%(nearby_ids)s)")
                    params["nearby_ids"] = nearby_ids or ["__none__"]
                except (ValueError, TypeError):
                    pass
            elif location:
                conditions.append("t.address ILIKE %(location)s")
                params["location"] = f"%{location.split(',')[0]}%"
            if score_col:
                conditions.append(f"e.{score_col} >= %(min_score)s")
                params["min_score"] = min_score
            if view in ("want", "been"):
                conditions.append("f.list_type = %(view)s")
                params["view"] = view
            if cuisine_filter:
                # Expand filter: Jina neighbors + word-overlap (e.g. "Weinbar" → "Bodega & Weinbar")
                explicit = set([cuisine_filter] + cuisine_neighbors.get(cuisine_filter, []))
                word_matches = {
                    ct for ct in cuisine_neighbors
                    if ct not in explicit and _cuisine_covers(cuisine_filter, ct, cuisine_neighbors)
                }
                expanded_cuisines = list(explicit | word_matches)
                conditions.append("e.cuisine_type = ANY(%(cuisine_types)s)")
                params["cuisine_types"] = expanded_cuisines
            if price_filter == "low":
                conditions.append("e.avg_price_pp IS NOT NULL AND e.avg_price_pp <= 15")
            elif price_filter == "mid":
                conditions.append("e.avg_price_pp IS NOT NULL AND e.avg_price_pp > 15 AND e.avg_price_pp <= 40")
            elif price_filter == "high":
                conditions.append("e.avg_price_pp IS NOT NULL AND e.avg_price_pp > 40 AND e.avg_price_pp <= 70")
            elif price_filter == "vhigh":
                conditions.append("e.avg_price_pp IS NOT NULL AND e.avg_price_pp > 70")

            where = " AND ".join(conditions)
            if semantic_order:
                # Preserve semantic ranking: array_position returns the index in the
                # ordered semantic_ids list, so rank-1 result appears first.
                order = "array_position(%(semantic_ids)s::text[], t.place_id), t.rating DESC"
            elif sort == "quality":
                order = "quality_score DESC NULLS LAST, t.rating DESC"
            elif sort == "value":
                # Value index: rewards high value_score AND low price.
                # value_score (1-10) weighted 65%, inverse price tier 35%.
                # Price normalised: 0€→10pts, 100€→0pts (capped).
                order = """(
                    COALESCE(e.value_score, 5.0) * 0.65
                    + (10.0 - LEAST(COALESCE(e.avg_price_pp, 33)::float / 10.0, 10.0)) * 0.35
                ) DESC NULLS LAST, t.rating DESC"""
            elif score_col:
                order = f"e.{score_col} DESC, t.rating DESC"
            else:
                order = "e.curation_score DESC NULLS LAST, t.rating DESC"

            # Count total matching rows for pagination (no quality CTE needed)
            cur.execute(f"""
                SELECT COUNT(*) AS n
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                LEFT JOIN serpapi_details    sd ON sd.place_id = t.place_id
                LEFT JOIN restaurants       res ON res.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                               AND f.session_id = %(sid)s
                WHERE {where}
            """, params)
            total_filtered = cur.fetchone()["n"]
            total_pages    = max(1, (total_filtered + PER_PAGE - 1) // PER_PAGE)
            page           = min(page, total_pages)
            offset         = (page - 1) * PER_PAGE
            ctx["total_filtered"] = total_filtered
            ctx["total_pages"]    = total_pages
            ctx["page"]           = page

            # ── Quality sort: prepend CTEs that compute 3-pillar quality score ──
            # quality_score = 0.45 × critic_score_zscore
            #               + 0.35 × bayesian_google_zscore   (50-prior smoothing)
            #               + 0.20 × value_score_zscore
            if sort == "quality":
                quality_cte = """
                    WITH quality_stats AS (
                        SELECT
                            AVG(e2.critic_score)::float     AS avg_critic,
                            STDDEV(e2.critic_score)::float  AS std_critic,
                            AVG(e2.value_score)::float      AS avg_value,
                            STDDEV(e2.value_score)::float   AS std_value,
                            AVG(tr.rating)::float           AS global_mean_rating
                        FROM top_restaurants tr
                        JOIN gemini_enrichments e2 ON e2.place_id = tr.place_id
                        WHERE e2.critic_score IS NOT NULL AND e2.value_score IS NOT NULL
                    ),
                    bayesian_base AS (
                        SELECT
                            tr.place_id,
                            (50.0 * qs.global_mean_rating
                             + tr.rating_count * tr.rating)::float
                            / (50.0 + tr.rating_count) AS bayes_rating
                        FROM top_restaurants tr
                        CROSS JOIN quality_stats qs
                    ),
                    bayesian_stats AS (
                        SELECT
                            AVG(bayes_rating)::float    AS avg_bayes,
                            STDDEV(bayes_rating)::float AS std_bayes
                        FROM bayesian_base
                    )
                """
                quality_col = """,
                    CASE WHEN e.critic_score IS NOT NULL AND e.value_score IS NOT NULL THEN
                        0.45 * (e.critic_score    - qs.avg_critic) / NULLIF(qs.std_critic, 0.001)
                      + 0.35 * (bb.bayes_rating   - bs.avg_bayes)  / NULLIF(bs.std_bayes,  0.001)
                      + 0.20 * (e.value_score      - qs.avg_value)  / NULLIF(qs.std_value,  0.001)
                    END AS quality_score"""
                quality_joins = """
                    CROSS JOIN quality_stats qs
                    LEFT  JOIN bayesian_base  bb ON bb.place_id = t.place_id
                    CROSS JOIN bayesian_stats bs"""
            else:
                quality_cte   = ""
                quality_col   = ""
                quality_joins = ""

            cur.execute(f"""
                {quality_cte}
                SELECT
                    t.*,
                    e.family_score,  e.date_score,    e.friends_score, e.solo_score,
                    e.relaxed_score, e.party_score,   e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.dresscode_score,
                    e.outdoor_score, e.view_score,
                    e.cuisine_score, e.service_score, e.value_score,
                    e.ambiance_score, e.critic_score,
                    e.audience_type, e.avg_price_pp,
                    e.cuisine_type,  e.cuisine_tags,
                    e.summary_de, e.must_order, e.vibe,
                    sd.highlights, sd.popular_for, sd.offerings, sd.atmosphere,
                    sd.crowd, sd.planning, sd.amenities, sd.dining_options,
                    sd.service_options,
                    res.raw_data->>'type' AS place_type,
                    f.list_type             AS fav_type,
                    f.score                 AS fav_score,
                    (ea.slug IS NOT NULL)   AS has_article,
                    ea.slug                 AS article_slug
                    {quality_col}
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                LEFT JOIN serpapi_details    sd ON sd.place_id = t.place_id
                LEFT JOIN restaurants       res ON res.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                               AND f.session_id = %(sid)s
                LEFT JOIN editorial_articles ea ON ea.place_id = t.place_id
                                               AND ea.is_published = TRUE
                {quality_joins}
                WHERE {where}
                ORDER BY {order}
                LIMIT %(per_page)s OFFSET %(offset)s
            """, {**params, "per_page": PER_PAGE, "offset": offset})

            rows = cur.fetchall()
            restaurants = []
            for row in rows:
                r = dict(row)
                r["city"] = _extract_city(r.get("address"))
                r["type_emoji"], r["type_label"] = _classify_type(r.get("place_type"))
                r.setdefault("quality_score", None)

                # Load cached photos from Docker volume (downloaded during scraping)
                place_id_r = r["place_id"]
                photo_dir  = os.path.join(PHOTOS_DIR, place_id_r)
                photos: list[str] = []
                if os.path.isdir(photo_dir):
                    # SerpAPI photos: 0.jpg, 1.jpg, …
                    for i in range(MAX_PHOTOS):
                        path = os.path.join(photo_dir, f"{i}.jpg")
                        if os.path.exists(path):
                            photos.append(f"/static/photos/{place_id_r}/{i}.jpg")
                        else:
                            break
                    # Website scraper photos: 0_websiteScraper.jpg, 1_websiteScraper.jpg, …
                    ws_files = sorted(
                        f for f in os.listdir(photo_dir) if f.endswith("_websiteScraper.jpg")
                    )
                    for f in ws_files:
                        photos.append(f"/static/photos/{place_id_r}/{f}")

                # Fall back to the single Serper thumbnail if no cached photos
                if not photos and r.get("thumbnail_url"):
                    photos = [r["thumbnail_url"]]
                r["photos"] = photos

                restaurants.append(r)
            ctx["restaurants"] = restaurants

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    resp = make_response(render_template("index.html", **ctx))
    if new_sid:
        resp.set_cookie("sid", sid, max_age=365 * 24 * 3600, samesite="Lax", httponly=True)
    return resp


# ── Editorial "Unsere Tipps" ──────────────────────────────────────────────────

def _md_to_html(md: str, preview_only: bool = False) -> str:
    """Convert markdown to HTML. If preview_only, return first 3 paragraphs."""
    # Strip Gemini Deep Research citation markers: [cite: 1], [cite: 1, 2], etc.
    md = re.sub(r"\s*\[cite:\s*[\d,\s]+\]", "", md)

    if preview_only:
        paragraphs = []
        for line in md.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if stripped:
                paragraphs.append(stripped)
            if len(paragraphs) >= 3:
                break
        md = "\n\n".join(paragraphs)
    return markdown2.markdown(md, extras=["fenced-code-blocks", "strike", "tables"])


def _build_collection_query(col: dict) -> tuple[str, dict]:
    """Return (WHERE fragment, params dict) for a collection's filter rule."""
    ft  = col["filter_type"]
    fv  = col["filter_value"]
    ms  = col["min_score"] or 7
    if ft == "audience":
        return "e.audience_type = %(fv)s", {"fv": fv}
    if ft == "cuisine":
        return "e.cuisine_type ILIKE %(fv)s", {"fv": f"%{fv}%"}
    if ft == "profile":
        # fv is the score column prefix, e.g. "date" → e.date_score
        return f"e.{fv}_score >= %(ms)s", {"ms": ms}
    if ft == "city":
        return "r.address ILIKE %(fv)s", {"fv": f"%{fv}%"}
    if ft == "search":
        return (
            "(r.name ILIKE %(fv)s OR e.summary_de ILIKE %(fv)s OR e.cuisine_type ILIKE %(fv)s)",
            {"fv": f"%{fv}%"},
        )
    if ft == "tag":
        return (
            "(%(fv)s = ANY(e.cuisine_tags) OR %(fv)s = ANY(e.food_tags))",
            {"fv": fv},
        )
    raise ValueError(f"Unknown filter_type: {ft!r}")


@app.route("/<city>/listen")
def listen_index(city):
    """Curated collections overview page."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, slug, title, subtitle, emoji, filter_type, filter_value, min_score
                FROM collections
                WHERE is_published = TRUE
                  AND city_id = %(city_id)s
                ORDER BY sort_order
            """, {"city_id": city_data["id"]})
            cols_raw = [dict(r) for r in cur.fetchall()]

            collections = []
            for col in cols_raw:
                try:
                    where, params = _build_collection_query(col)
                except ValueError:
                    continue

                # Count matching restaurants
                cur.execute(f"""
                    SELECT COUNT(*) AS cnt
                    FROM restaurants r
                    JOIN gemini_enrichments e ON e.place_id = r.place_id
                    WHERE r.pipeline_status = 'complete' AND r.is_active = TRUE
                      AND r.city_id = %(city_id)s
                      AND {where}
                """, dict(params, city_id=city_data["id"]))
                col["count"] = cur.fetchone()["cnt"]

                # First photo from the top restaurant in this collection
                cur.execute(f"""
                    SELECT r.place_id, r.thumbnail_url
                    FROM restaurants r
                    JOIN gemini_enrichments e ON e.place_id = r.place_id
                    WHERE r.pipeline_status = 'complete' AND r.is_active = TRUE
                      AND r.city_id = %(city_id)s
                      AND {where}
                    ORDER BY e.curation_score DESC NULLS LAST
                    LIMIT 1
                """, dict(params, city_id=city_data["id"]))
                top = cur.fetchone()
                col["photo"] = None
                if top:
                    col["photo"] = _photo_url(top["place_id"], top["thumbnail_url"])

                collections.append(col)
    finally:
        conn.close()
    return render_template("listen.html", collections=collections, city=city_data)


@app.route("/<city>/listen/<slug>")
def listen_collection(city, slug):
    """Single curated collection page."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    sid = request.cookies.get("sid") or ""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, slug, title, subtitle, emoji, filter_type, filter_value, min_score
                FROM collections
                WHERE slug = %s AND is_published = TRUE
                  AND city_id = %s
            """, (slug, city_data["id"]))
            col = cur.fetchone()
        if not col:
            return redirect(f"/{city}/listen")

        col = dict(col)
        where, params = _build_collection_query(col)
        query_params = dict(params, sid=sid, city_id=city_data["id"])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT
                    r.place_id, r.name, r.address, r.rating, r.rating_count,
                    r.thumbnail_url, r.price_level,
                    r.raw_data->>'type' AS place_type,
                    e.cuisine_type, e.avg_price_pp, e.vibe, e.must_order,
                    e.curation_score,
                    e.family_score, e.date_score, e.friends_score, e.solo_score,
                    e.relaxed_score, e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.outdoor_score, e.view_score,
                    e.summary_de,
                    sd.popular_for, sd.offerings, sd.atmosphere, sd.highlights,
                    ea.slug AS article_slug,
                    (ea.place_id IS NOT NULL) AS has_article,
                    f.list_type AS fav_type,
                    f.score     AS fav_score
                FROM restaurants r
                JOIN gemini_enrichments e ON e.place_id = r.place_id
                LEFT JOIN serpapi_details sd ON sd.place_id = r.place_id
                LEFT JOIN editorial_articles ea
                    ON ea.place_id = r.place_id AND ea.is_published = TRUE
                LEFT JOIN user_favorites f
                    ON f.place_id = r.place_id AND f.session_id = %(sid)s
                WHERE r.pipeline_status = 'complete' AND r.is_active = TRUE
                  AND r.city_id = %(city_id)s
                  AND {where}
                ORDER BY e.curation_score DESC NULLS LAST
                LIMIT 10
            """, query_params)
            rows = cur.fetchall()

        restaurants = []
        for row in rows:
            row = dict(row)
            row = _enrich_row(row)
            restaurants.append(row)

        col["restaurant_count"] = len(restaurants)
    finally:
        conn.close()
    return render_template("listen_collection.html", col=col, restaurants=restaurants, city=city_data)


@app.route("/tipps")
def tipps_all():
    """Editorial blog listing — all cities combined."""
    _init_db()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    a.slug, a.title, a.teaser, a.generated_at,
                    r.place_id, r.name, r.address, r.rating, r.rating_count,
                    r.thumbnail_url,
                    e.cuisine_type, e.avg_price_pp, e.vibe,
                    c.slug AS city_slug,
                    e.curation_score
                FROM editorial_articles a
                JOIN restaurants r ON r.place_id = a.place_id
                JOIN cities c ON c.id = a.city_id
                LEFT JOIN gemini_enrichments e ON e.place_id = a.place_id
                WHERE a.is_published = TRUE
                  AND c.is_published = TRUE
                ORDER BY e.curation_score DESC NULLS LAST, r.rating DESC NULLS LAST
            """)
            articles = []
            for row in cur.fetchall():
                row = dict(row)
                row["city"] = _extract_city(row.get("address"))
                pid = row["place_id"]
                row["photo"] = _photo_url(pid, row.get("thumbnail_url"))
                articles.append(row)
    finally:
        conn.close()
    return render_template("tipps.html", articles=articles, city=None)


@app.route("/<city>/tipps")
def tipps_index(city):
    """Editorial blog listing page."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    a.slug, a.title, a.teaser, a.generated_at,
                    r.place_id, r.name, r.address, r.rating, r.rating_count,
                    r.thumbnail_url,
                    e.cuisine_type, e.avg_price_pp, e.vibe,
                    c.slug AS city_slug,
                    e.curation_score
                FROM editorial_articles a
                JOIN restaurants r ON r.place_id = a.place_id
                JOIN cities c ON c.id = a.city_id
                LEFT JOIN gemini_enrichments e ON e.place_id = a.place_id
                WHERE a.is_published = TRUE
                  AND a.city_id = %(city_id)s
                ORDER BY e.curation_score DESC NULLS LAST, r.rating DESC NULLS LAST
            """, {"city_id": city_data["id"]})
            articles = []
            for row in cur.fetchall():
                row = dict(row)
                row["city"] = _extract_city(row.get("address"))
                pid = row["place_id"]
                row["photo"] = _photo_url(pid, row.get("thumbnail_url"))
                articles.append(row)
    finally:
        conn.close()
    return render_template("tipps.html", articles=articles, city=city_data)


@app.route("/<city>/tipps/<slug>")
def tipps_article(city, slug):
    """Single editorial article page."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    a.slug, a.title, a.article_md, a.teaser, a.generated_at, a.gemini_model,
                    r.place_id, r.name, r.address, r.rating, r.rating_count,
                    r.website, r.phone, r.latitude, r.longitude, r.thumbnail_url,
                    e.cuisine_type, e.avg_price_pp, e.vibe, e.summary_de,
                    e.cuisine_tags, e.audience_type
                FROM editorial_articles a
                JOIN restaurants r ON r.place_id = a.place_id
                LEFT JOIN gemini_enrichments e ON e.place_id = a.place_id
                WHERE a.slug = %s AND a.is_published = TRUE
                  AND a.city_id = %s
            """, (slug, city_data["id"]))
            article = cur.fetchone()

        if not article:
            return redirect(f"/{city}/tipps")

        article = dict(article)
        article["city"] = _extract_city(article.get("address"))
        article["article_html"] = _md_to_html(article["article_md"])
        pid = article["place_id"]

        # Load photos
        photo_dir = os.path.join(PHOTOS_DIR, pid)
        photos: list[str] = []
        if os.path.isdir(photo_dir):
            for i in range(MAX_PHOTOS):
                path = os.path.join(photo_dir, f"{i}.jpg")
                if os.path.exists(path):
                    photos.append(f"/static/photos/{pid}/{i}.jpg")
                else:
                    break
            ws_files = sorted(f for f in os.listdir(photo_dir) if f.endswith("_websiteScraper.jpg"))
            for f in ws_files:
                photos.append(f"/static/photos/{pid}/{f}")
        if not photos and article.get("thumbnail_url"):
            photos = [article["thumbnail_url"]]
        article["photos"] = photos

        # Load 3 other published articles for "Mehr Tipps" footer
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT a.slug, a.title, a.teaser,
                       r.place_id, r.name, r.address, r.thumbnail_url,
                       e.cuisine_type
                FROM editorial_articles a
                JOIN restaurants r ON r.place_id = a.place_id
                LEFT JOIN gemini_enrichments e ON e.place_id = a.place_id
                WHERE a.is_published = TRUE AND a.slug != %s
                  AND a.city_id = %s
                ORDER BY a.generated_at DESC
                LIMIT 3
            """, (slug, city_data["id"]))
            more = []
            for row in cur.fetchall():
                row = dict(row)
                row["city"] = _extract_city(row.get("address"))
                row["photo"] = _photo_url(row["place_id"], row.get("thumbnail_url"))
                more.append(row)
        article["more"] = more

    finally:
        conn.close()
    return render_template("tipps_article.html", a=article, city=city_data)


@app.route("/<city>/küchen")
def kuechen_index(city):
    """Cuisine discovery page: DNA labels with top restaurants per cuisine."""
    city_data = _get_city_or_404(city)
    g.current_city = city_data
    _init_db()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT label, cuisine_types, wpmi, restaurant_n
                FROM city_cuisine_labels
                WHERE city_id = %(city_id)s
                ORDER BY wpmi DESC
            """, {"city_id": city_data["id"]})
            labels = [dict(r) for r in cur.fetchall()]

            for lbl in labels:
                cur.execute("""
                    SELECT
                        r.place_id, r.name, r.address, r.rating,
                        r.rating_count, r.thumbnail_url,
                        e.avg_price_pp, e.vibe, e.cuisine_type, e.curation_score
                    FROM top_restaurants t
                    JOIN restaurants r ON r.place_id = t.place_id
                    JOIN gemini_enrichments e ON e.place_id = r.place_id
                    WHERE t.city_id = %(city_id)s
                      AND e.cuisine_type = ANY(%(cuisine_types)s)
                    ORDER BY e.curation_score DESC NULLS LAST, r.rating DESC NULLS LAST
                    LIMIT 6
                """, {"city_id": city_data["id"], "cuisine_types": lbl["cuisine_types"]})
                restaurants = []
                for row in cur.fetchall():
                    row = dict(row)
                    row["photo"] = _photo_url(row["place_id"], row.get("thumbnail_url"))
                    restaurants.append(row)
                lbl["restaurants"] = restaurants

            labels = [l for l in labels if l["restaurants"]]
    finally:
        conn.close()
    return render_template("kuechen.html", labels=labels, city=city_data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)

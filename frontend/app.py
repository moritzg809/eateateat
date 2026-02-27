import os
import re
import time as _time
import uuid

import numpy as np
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, jsonify, make_response

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mallorcaeat-dev-key")

PHOTOS_DIR  = "/app/static/photos"   # Docker volume mount (same as scraper's /photos)
MAX_PHOTOS  = 8
PER_PAGE    = 24

PROFILES = [
    ("family",   "👨‍👩‍👧", "Familie"),
    ("date",     "💑",     "Date Night"),
    ("friends",  "👯",     "Friends Trip"),
    ("solo",     "🧍",     "Solo"),
    ("relaxed",  "😌",     "Entspannt"),
    ("party",    "🎉",     "Party Vibe"),
    ("special",  "✨",     "Besonderer Anlass"),
    ("foodie",   "🍽️",    "Foodie"),
    ("lingering","☕",     "Verweilen"),
    ("unique",   "💎",     "Geheimtipp"),
    ("dresscode","👔",     "Dress Code"),
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


# ── Recommender — candidate cache (module-level, refreshed every 5 min) ──────

_CAND_CACHE: tuple[float, list[dict]] | None = None
_CAND_CACHE_TTL = 300  # seconds

_SCORE_COLS = [
    "family_score", "date_score",   "friends_score", "solo_score",
    "relaxed_score","party_score",  "special_score", "foodie_score",
    "lingering_score","unique_score","dresscode_score",
]


def _load_candidates(conn) -> list[dict]:
    """Load all top restaurants with scores, embeddings and open_slots.

    Results are cached module-wide for _CAND_CACHE_TTL seconds so repeated
    calls to /api/similar don't hammer the DB with 6 MB queries.
    Each row is pre-processed:
      row['scores_norm']  – normalised numpy score vector (or None)
      row['emb_norm']     – normalised embedding vector   (or None)
    """
    global _CAND_CACHE
    now = _time.time()
    if _CAND_CACHE and now - _CAND_CACHE[0] < _CAND_CACHE_TTL:
        return _CAND_CACHE[1]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                r.place_id,
                r.name,
                r.rating,
                r.price_level,
                r.thumbnail_url,
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
                e.vibe,
                emb.embedding
            FROM  top_restaurants           r
            JOIN  restaurants             res ON res.place_id = r.place_id
            LEFT JOIN gemini_enrichments    e ON e.place_id   = r.place_id
            LEFT JOIN restaurant_embeddings emb ON emb.place_id = r.place_id
        """)
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

        processed.append(row)

    _CAND_CACHE = (_time.time(), processed)
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


# ── Recommender API ──────────────────────────────────────────────────────────

@app.route("/api/similar/<place_id>")
def api_similar(place_id):
    """Return up to n restaurants similar to <place_id>.

    Composite similarity — two modes:

    With embeddings (when target + candidate both have Gemini embeddings):
      0.55 × embedding cosine    (text: description, types, tags, summary, vibe, …)
      0.20 × profile-score cosine (11 Gemini dimension scores)
      0.15 × open-hours Jaccard  (2-h time-slot overlap)
      0.05 × type bonus
      0.05 × price bonus

    Fallback (SQL, when embeddings unavailable):
      0.50 × profile-score cosine
      0.20 × SerpAPI tag Jaccard
      0.15 × open-hours Jaccard
      0.10 × type bonus
      0.05 × price bonus

    Only results with similarity ≥ 0.50 are returned.
    """
    n    = min(int(request.args.get("n", 6)), 20)
    conn = get_db()
    try:
        candidates = _load_candidates(conn)
    finally:
        conn.close()

    # ── Find target ───────────────────────────────────────────────────────────
    target = next((c for c in candidates if c["place_id"] == place_id), None)
    if not target or target["scores_norm"] is None:
        return jsonify([])  # no scores → can't rank

    t_scores  = target["scores_norm"]
    t_emb     = target["emb_norm"]
    t_slots   = target["slots_set"]
    t_type    = target["place_type"]
    t_price   = target["price_level"]
    use_emb   = t_emb is not None

    # ── Score each candidate ──────────────────────────────────────────────────
    ranked: list[tuple[float, dict]] = []

    for cand in candidates:
        if cand["place_id"] == place_id:
            continue
        if cand["scores_norm"] is None:
            continue

        # Profile-score cosine (always available)
        score_cos = float(np.dot(t_scores, cand["scores_norm"]))

        # Embedding cosine (only when both have embeddings)
        emb_cos = 0.0
        c_emb   = cand["emb_norm"]
        if use_emb and c_emb is not None:
            emb_cos = float(np.dot(t_emb, c_emb))

        # Open-hours Jaccard
        slots_j = _jaccard(t_slots, cand["slots_set"])

        # Type & price bonuses
        type_b  = 0.05 if cand["place_type"] == t_type else 0.0
        price_b = _price_bonus(t_price, cand["price_level"])

        if use_emb and c_emb is not None:
            # Full embedding mode
            sim = (0.55 * emb_cos
                 + 0.20 * score_cos
                 + 0.15 * slots_j
                 + type_b + price_b)
        else:
            # Scores-only mode (no embedding for target or candidate)
            sim = (0.60 * score_cos
                 + 0.15 * slots_j
                 + type_b + price_b * 2)   # bump price weight slightly

        if sim >= 0.50:
            ranked.append((sim, cand))

    ranked.sort(key=lambda x: x[0], reverse=True)

    # ── Build response ────────────────────────────────────────────────────────
    results = []
    for sim, cand in ranked[:n]:
        pid        = cand["place_id"]
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


# ── Main view ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    _init_db()

    # Session cookie (anonymous, persistent)
    sid     = request.cookies.get("sid") or ""
    new_sid = not sid
    if new_sid:
        sid = str(uuid.uuid4())

    search      = request.args.get("search", "").strip()
    location    = request.args.get("location", "").strip()
    profile_key = request.args.get("profile", "").strip()
    min_score   = int(request.args.get("min_score", 7))
    page        = max(1, int(request.args.get("page", 1) or 1))
    view        = request.args.get("view", "").strip()  # "" | "want" | "been"

    ctx = {
        "total": 0, "top_count": 0, "avg_rating": "–", "enriched_count": 0,
        "restaurants": [], "locations": [],
        "search": search, "location": location,
        "profile_key": profile_key, "min_score": min_score,
        "profiles": PROFILES,
        "page": page, "total_pages": 1, "total_filtered": 0, "per_page": PER_PAGE,
        "view": view,
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

            # Build main query
            score_col  = f"{profile_key}_score" if profile_key else None
            conditions = ["1=1"]
            params: dict = {"sid": sid}

            if search:
                conditions.append("t.name ILIKE %(search)s")
                params["search"] = f"%{search}%"
            if location:
                conditions.append("t.address ILIKE %(location)s")
                params["location"] = f"%{location.split(',')[0]}%"
            if score_col:
                conditions.append(f"e.{score_col} >= %(min_score)s")
                params["min_score"] = min_score
            if view in ("want", "been"):
                conditions.append("f.list_type = %(view)s")
                params["view"] = view

            where = " AND ".join(conditions)
            order = f"e.{score_col} DESC, t.rating DESC" if score_col else "t.rating DESC, t.rating_count DESC"

            # Count total matching rows for pagination
            cur.execute(f"""
                SELECT COUNT(*) AS n
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id = t.place_id
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

            cur.execute(f"""
                SELECT
                    t.*,
                    e.family_score, e.date_score,    e.friends_score, e.solo_score,
                    e.relaxed_score, e.party_score,  e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.dresscode_score,
                    e.summary_de, e.must_order, e.vibe,
                    sd.highlights, sd.popular_for, sd.offerings, sd.atmosphere,
                    sd.crowd, sd.planning, sd.amenities, sd.dining_options,
                    sd.service_options,
                    res.raw_data->>'type' AS place_type,
                    f.list_type             AS fav_type,
                    f.score                 AS fav_score
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e  ON e.place_id  = t.place_id
                LEFT JOIN serpapi_details    sd ON sd.place_id = t.place_id
                LEFT JOIN restaurants       res ON res.place_id = t.place_id
                LEFT JOIN user_favorites      f ON f.place_id  = t.place_id
                                               AND f.session_id = %(sid)s
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

                # Load cached photos from Docker volume (downloaded during scraping)
                place_id_r = r["place_id"]
                photo_dir  = os.path.join(PHOTOS_DIR, place_id_r)
                photos: list[str] = []
                if os.path.isdir(photo_dir):
                    for i in range(MAX_PHOTOS):
                        path = os.path.join(photo_dir, f"{i}.jpg")
                        if os.path.exists(path):
                            photos.append(f"/static/photos/{place_id_r}/{i}.jpg")
                        else:
                            break  # files are numbered consecutively

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)

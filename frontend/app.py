import os
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request

app = Flask(__name__)

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


def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@app.route("/")
def index():
    search      = request.args.get("search", "").strip()
    location    = request.args.get("location", "").strip()
    profile_key = request.args.get("profile", "").strip()
    min_score   = int(request.args.get("min_score", 7))

    ctx = {
        "total": 0, "top_count": 0, "avg_rating": "–", "enriched_count": 0,
        "restaurants": [], "locations": [],
        "search": search, "location": location,
        "profile_key": profile_key, "min_score": min_score,
        "profiles": PROFILES,
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
            score_col = f"{profile_key}_score" if profile_key else None
            conditions = ["1=1"]
            params: dict = {}

            if search:
                conditions.append("t.name ILIKE %(search)s")
                params["search"] = f"%{search}%"
            if location:
                conditions.append("t.address ILIKE %(location)s")
                params["location"] = f"%{location.split(',')[0]}%"
            if score_col:
                conditions.append(f"e.{score_col} >= %(min_score)s")
                params["min_score"] = min_score

            where = " AND ".join(conditions)
            order = f"e.{score_col} DESC, t.rating DESC" if score_col else "t.rating DESC, t.rating_count DESC"

            cur.execute(f"""
                SELECT
                    t.*,
                    e.family_score, e.date_score,    e.friends_score, e.solo_score,
                    e.relaxed_score, e.party_score,  e.special_score, e.foodie_score,
                    e.lingering_score, e.unique_score, e.dresscode_score,
                    e.summary_de, e.must_order, e.vibe
                FROM top_restaurants t
                LEFT JOIN gemini_enrichments e ON e.place_id = t.place_id
                WHERE {where}
                ORDER BY {order}
                LIMIT 120
            """, params)
            ctx["restaurants"] = cur.fetchall()

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    return render_template("index.html", **ctx)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)

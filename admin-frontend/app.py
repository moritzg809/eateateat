"""
mallorcaeat Admin Frontend — Pipeline Status Dashboard
Runs on port 8081 (separate from main frontend on 8080).
"""

import os
import re as _re
import threading
import time as _time
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "mallorcaeat-admin-dev-secret")

DATABASE_URL = os.environ["DATABASE_URL"]

REVIEW_PAGE_SIZE = 50


def get_db():
    return psycopg2.connect(DATABASE_URL)


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard index
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    ctx = {}
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ── Pipeline funnel counts ──────────────────────────────────────
            cur.execute(
                """
                SELECT
                    pipeline_status,
                    count(*) AS n,
                    count(*) FILTER (WHERE is_active = FALSE) AS inactive_n
                FROM restaurants
                GROUP BY pipeline_status
                ORDER BY pipeline_status
                """
            )
            status_rows = cur.fetchall()
            status_counts = {r["pipeline_status"]: r["n"] for r in status_rows}
            ctx["status_counts"] = status_counts
            ctx["total_restaurants"] = sum(status_counts.values())

            # Friendly funnel summary
            ctx["funnel"] = {
                "scraped":      sum(status_counts.get(s, 0)
                                    for s in ("new", "disqualified", "enriched", "complete", "inactive")),
                "qualified":    sum(status_counts.get(s, 0)
                                    for s in ("enriched", "complete")),
                "enriched":     status_counts.get("enriched", 0),
                "complete":     status_counts.get("complete", 0),
                "disqualified": status_counts.get("disqualified", 0),
                "inactive":     status_counts.get("inactive", 0),
            }

            # ── Daily enrichment gauge ──────────────────────────────────────
            cur.execute(
                "SELECT count(*) FROM gemini_enrichments WHERE enriched_at::date = CURRENT_DATE"
            )
            ctx["today_enrichments"] = cur.fetchone()["count"]
            ctx["daily_limit"] = 500

            # ── SerpAPI details coverage ────────────────────────────────────
            cur.execute(
                """
                SELECT
                    count(*) FILTER (WHERE sd.place_id IS NOT NULL)     AS with_details,
                    count(*) FILTER (WHERE sd.place_id IS NULL)         AS without_details
                FROM   restaurants r
                LEFT JOIN serpapi_details sd ON sd.place_id = r.place_id
                WHERE  r.pipeline_status = 'complete'
                """
            )
            row = cur.fetchone()
            ctx["details_coverage"] = dict(row) if row else {}

            # ── Gem qualify stats ───────────────────────────────────────────
            try:
                cur.execute(
                    """
                    SELECT
                        count(*)                                                        AS total_evaluated,
                        count(*) FILTER (WHERE qualified = TRUE  AND rejected = FALSE)  AS pending_review,
                        count(*) FILTER (WHERE qualified = TRUE  AND rejected = TRUE)   AS rejected_count,
                        count(*) FILTER (WHERE qualified = FALSE)                       AS not_qualified
                    FROM gemini_prequalify
                    """
                )
                gq = cur.fetchone()
                ctx["gem_qualify"] = dict(gq) if gq else {}

                # Actionable = qualified, not rejected, still disqualified in pipeline
                cur.execute(
                    """
                    SELECT count(*)
                    FROM   gemini_prequalify p
                    JOIN   restaurants r ON r.place_id = p.place_id
                    WHERE  p.qualified = TRUE AND p.rejected = FALSE
                      AND  r.pipeline_status = 'disqualified'
                    """
                )
                ctx["gem_qualify"]["actionable"] = cur.fetchone()["count"]

                # Today's prequalify calls (for shared quota display)
                cur.execute(
                    "SELECT count(*) FROM gemini_prequalify WHERE evaluated_at::date = CURRENT_DATE"
                )
                ctx["today_prequalify"] = cur.fetchone()["count"]
            except Exception:
                ctx["gem_qualify"] = {}
                ctx["today_prequalify"] = 0

            # ── Pipeline runs table ─────────────────────────────────────────
            cur.execute(
                """
                SELECT
                    query,
                    location,
                    last_run_at,
                    result_count,
                    status,
                    CASE
                        WHEN last_run_at IS NULL THEN 'never'
                        WHEN last_run_at < NOW() - INTERVAL '6 months' THEN 'overdue'
                        ELSE 'ok'
                    END AS freshness,
                    CASE
                        WHEN last_run_at IS NOT NULL
                        THEN GREATEST(0, EXTRACT(DAY FROM
                            (last_run_at + INTERVAL '6 months') - NOW()
                        ))::int
                        ELSE NULL
                    END AS days_until_due
                FROM pipeline_runs
                ORDER BY
                    CASE WHEN last_run_at IS NULL THEN 0
                         WHEN last_run_at < NOW() - INTERVAL '6 months' THEN 1
                         ELSE 2 END,
                    last_run_at ASC NULLS FIRST
                """
            )
            ctx["pipeline_runs"] = cur.fetchall()
            ctx["runs_overdue"] = sum(1 for r in ctx["pipeline_runs"]
                                      if r["freshness"] in ("never", "overdue"))
            ctx["runs_total"] = len(ctx["pipeline_runs"])

            # ── Recently inactive restaurants ───────────────────────────────
            cur.execute(
                """
                SELECT name, address, rating, rating_count, last_verified_at
                FROM   restaurants
                WHERE  is_active = FALSE OR pipeline_status = 'inactive'
                ORDER  BY last_verified_at DESC NULLS LAST
                LIMIT  20
                """
            )
            ctx["inactive_restaurants"] = cur.fetchall()

            # ── Restaurants pending verify (> 2 years) ──────────────────────
            cur.execute(
                """
                SELECT count(*) AS n
                FROM   restaurants
                WHERE  pipeline_status = 'complete'
                  AND  (last_verified_at IS NULL
                        OR last_verified_at < NOW() - INTERVAL '2 years')
                """
            )
            ctx["verify_due_count"] = cur.fetchone()["n"]

            # ── Last pipeline run (most recent serper_cache update) ─────────
            cur.execute("SELECT MAX(last_run_at) FROM pipeline_runs WHERE last_run_at IS NOT NULL")
            row = cur.fetchone()
            ctx["last_run_at"] = row["max"] if row else None

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    ctx["now"] = datetime.now(timezone.utc)
    return render_template("index.html", **ctx)


# ─────────────────────────────────────────────────────────────────────────────
# Gem-qualify review queue
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/review")
def review():
    ctx = {}
    page = request.args.get("page", 1, type=int)
    offset = (page - 1) * REVIEW_PAGE_SIZE

    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # Total actionable candidates
            cur.execute(
                """
                SELECT count(*)
                FROM   gemini_prequalify p
                JOIN   restaurants r ON r.place_id = p.place_id
                WHERE  p.qualified = TRUE AND p.rejected = FALSE
                  AND  r.pipeline_status = 'disqualified'
                """
            )
            ctx["total"] = cur.fetchone()["count"]
            ctx["page"] = page
            ctx["page_size"] = REVIEW_PAGE_SIZE
            ctx["total_pages"] = max(1, (ctx["total"] + REVIEW_PAGE_SIZE - 1) // REVIEW_PAGE_SIZE)

            # Score distribution for the batch-threshold helper
            cur.execute(
                """
                SELECT
                    (p.unique_score + p.foodie_score) AS combined,
                    count(*) AS n
                FROM   gemini_prequalify p
                JOIN   restaurants r ON r.place_id = p.place_id
                WHERE  p.qualified = TRUE AND p.rejected = FALSE
                  AND  r.pipeline_status = 'disqualified'
                GROUP  BY combined
                ORDER  BY combined DESC
                """
            )
            ctx["score_distribution"] = cur.fetchall()

            # Candidates for this page, sorted best-first
            cur.execute(
                """
                SELECT
                    r.place_id,
                    r.name,
                    r.address,
                    r.rating,
                    r.rating_count,
                    r.website,
                    r.latitude,
                    r.longitude,
                    p.unique_score,
                    p.foodie_score,
                    (p.unique_score + p.foodie_score) AS combined_score,
                    p.evaluated_at
                FROM   gemini_prequalify p
                JOIN   restaurants r ON r.place_id = p.place_id
                WHERE  p.qualified = TRUE AND p.rejected = FALSE
                  AND  r.pipeline_status = 'disqualified'
                ORDER  BY (p.unique_score + p.foodie_score) DESC,
                           p.foodie_score DESC
                LIMIT  %s OFFSET %s
                """,
                (REVIEW_PAGE_SIZE, offset),
            )
            ctx["candidates"] = cur.fetchall()

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    ctx["now"] = datetime.now(timezone.utc)
    return render_template("review.html", **ctx)


@app.route("/review/approve", methods=["POST"])
def review_approve():
    min_score = request.form.get("min_score", type=int)
    place_ids = request.form.getlist("place_id")
    page      = request.form.get("page", 1)

    conn = get_db()
    approved = 0
    try:
        with conn.cursor() as cur:
            if min_score is not None:
                # Batch-approve all pending candidates with combined score >= threshold
                cur.execute(
                    """
                    UPDATE restaurants r
                    SET    pipeline_status = 'new'
                    FROM   gemini_prequalify p
                    WHERE  r.place_id = p.place_id
                      AND  r.pipeline_status  = 'disqualified'
                      AND  p.qualified        = TRUE
                      AND  p.rejected         = FALSE
                      AND  (p.unique_score + p.foodie_score) >= %s
                    """,
                    (min_score,),
                )
                approved = cur.rowcount
            elif place_ids:
                cur.execute(
                    """
                    UPDATE restaurants
                    SET    pipeline_status = 'new'
                    WHERE  place_id = ANY(%s)
                      AND  pipeline_status = 'disqualified'
                    """,
                    (place_ids,),
                )
                approved = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Genehmigen: {exc}", "error")
    finally:
        conn.close()

    if approved:
        flash(f"✓ {approved} Restaurant(s) zur Anreicherung freigegeben.", "success")
    return redirect(url_for("review", page=page))


@app.route("/review/reject", methods=["POST"])
def review_reject():
    max_score = request.form.get("max_score", type=int)
    place_ids = request.form.getlist("place_id")
    page      = request.form.get("page", 1)

    conn = get_db()
    rejected = 0
    try:
        with conn.cursor() as cur:
            if max_score is not None:
                # Batch-reject all pending candidates with combined score <= threshold
                cur.execute(
                    """
                    UPDATE gemini_prequalify p
                    SET    rejected = TRUE
                    FROM   restaurants r
                    WHERE  p.place_id = r.place_id
                      AND  r.pipeline_status = 'disqualified'
                      AND  p.qualified       = TRUE
                      AND  p.rejected        = FALSE
                      AND  (p.unique_score + p.foodie_score) <= %s
                    """,
                    (max_score,),
                )
                rejected = cur.rowcount
            elif place_ids:
                cur.execute(
                    "UPDATE gemini_prequalify SET rejected = TRUE WHERE place_id = ANY(%s)",
                    (place_ids,),
                )
                rejected = cur.rowcount
        conn.commit()
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Ablehnen: {exc}", "error")
    finally:
        conn.close()

    if rejected:
        flash(f"✗ {rejected} Restaurant(s) abgelehnt.", "info")
    return redirect(url_for("review", page=page))


# ─────────────────────────────────────────────────────────────────────────────
# Deep Research article generation
# ─────────────────────────────────────────────────────────────────────────────

GEMINI_DEEP_MODEL = "deep-research-pro-preview-12-2025"

ARTICLE_PROMPT = """
Schreibe einen hochwertigen deutschen Restaurantartikel über das Restaurant „{name}" in {city}.

Recherchiere:
- Der Küchenchef / die Gründer: Biografie, Ausbildung, Küchenstil, Philosophie
- Das Restaurant selbst: Geschichte, Atmosphäre, Lage, Auszeichnungen
- Die Küche: typische Gerichte, Konzept, lokale Tradition
- Praktische Infos: Preise, Reservierung, Öffnungszeiten
- Was das Restaurant für deutschsprachige Reisende besonders macht

Zielgruppe: deutschsprachige Reisende, die authentische Küche abseits des Massentourismus suchen.
Format: journalistischer Restaurantartikel mit einem prägnanten Titel (als H1), Einleitung, mehreren Abschnitten und Fazit.
Der Artikel soll zwischen 600 und 1200 Wörter lang sein.
"""

# In-memory job store: job_id → dict
_JOBS: dict[str, dict] = {}


def _slugify(name: str, city: str) -> str:
    s = f"{name}-{city}".lower()
    for src, dst in [("äàáâã", "a"), ("éèêë", "e"), ("íìîï", "i"),
                     ("öóòôõ", "o"), ("üùúû", "u"), ("ñ", "n"), ("ç", "c"), ("ß", "ss")]:
        for ch in src:
            s = s.replace(ch, dst)
    s = _re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _extract_title(md: str) -> str:
    for line in md.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    for line in md.splitlines():
        if line.strip():
            return line.strip()[:100]
    return "Unser Tipp"


def _extract_teaser(md: str) -> str:
    lines = md.splitlines()
    in_intro = False
    paragraphs = []
    for line in lines:
        s = line.strip()
        if s.startswith("#"):
            if in_intro and paragraphs:
                break
            in_intro = True
            continue
        if not s or s.startswith("*") or s.startswith("|") or s.startswith("-"):
            continue
        paragraphs.append(s)
        if len(paragraphs) >= 2:
            break
    teaser = " ".join(paragraphs)
    return teaser[:300] + ("…" if len(teaser) > 300 else "")


def _extract_city(address: str) -> str:
    m = _re.search(r"\d{5}\s+([^,]+)", address)
    if m:
        return m.group(1).strip()
    parts = [p.strip() for p in address.split(",")]
    return parts[-2] if len(parts) >= 2 else parts[0]


def _run_generation(job_id: str, place_id: str, name: str, address: str, city_id: int):
    """Background thread: generate article via Gemini Deep Research, then save to DB."""
    _JOBS[job_id]["status"] = "running"
    try:
        from google import genai as _genai

        city = _extract_city(address)
        prompt = ARTICLE_PROMPT.format(name=name, city=city)

        client = _genai.Client()
        interaction = client.interactions.create(
            input=prompt,
            agent=GEMINI_DEEP_MODEL,
            background=True,
        )
        _JOBS[job_id]["interaction_id"] = interaction.id

        while True:
            interaction = client.interactions.get(interaction.id)
            _JOBS[job_id]["gemini_status"] = interaction.status
            if interaction.status == "completed":
                break
            if interaction.status == "failed":
                raise RuntimeError(f"Deep Research fehlgeschlagen: {interaction.error}")
            _time.sleep(15)

        article_md = interaction.outputs[-1].text
        title = _extract_title(article_md)
        teaser = _extract_teaser(article_md)
        slug = _slugify(name, city)

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO editorial_articles
                    (place_id, slug, title, article_md, teaser, gemini_model, is_published, city_id)
                VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s)
                ON CONFLICT (place_id) DO UPDATE SET
                    slug         = EXCLUDED.slug,
                    title        = EXCLUDED.title,
                    article_md   = EXCLUDED.article_md,
                    teaser       = EXCLUDED.teaser,
                    gemini_model = EXCLUDED.gemini_model,
                    generated_at = NOW()
                """,
                (place_id, slug, title, article_md, teaser, GEMINI_DEEP_MODEL, city_id),
            )
        conn.commit()
        conn.close()

        _JOBS[job_id].update({"status": "done", "slug": slug, "title": title})

    except Exception as exc:
        _JOBS[job_id].update({"status": "error", "error": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# Küchen-Übersicht — DNA cuisine labels with top-5 restaurants + article gen
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/küchen")
def kuechen():
    city_id = request.args.get("city_id", 1, type=int)
    ctx: dict = {}
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("SELECT id, name, slug FROM cities ORDER BY id")
            ctx["cities"] = cur.fetchall()

            cur.execute("SELECT id, name, slug FROM cities WHERE id = %s", (city_id,))
            ctx["city"] = cur.fetchone()
            ctx["city_id"] = city_id

            # DNA cuisine labels ordered by distinctiveness
            cur.execute("""
                SELECT label, cuisine_types, wpmi, restaurant_n
                FROM city_cuisine_labels
                WHERE city_id = %s
                ORDER BY wpmi DESC
            """, (city_id,))
            labels = [dict(r) for r in cur.fetchall()]
            for lbl in labels:
                lbl["wpmi"] = float(lbl["wpmi"])
                lbl["wpmi_pct"] = min(int(lbl["wpmi"] / 3.0 * 100), 100)

            # Top 5 restaurants per label (by rating DESC), with article status
            for lbl in labels:
                cur.execute("""
                    SELECT
                        r.place_id, r.name, r.address, r.rating, r.rating_count,
                        r.thumbnail_url, r.website, r.latitude, r.longitude,
                        e.cuisine_type, e.avg_price_pp,
                        a.slug     AS article_slug,
                        a.title    AS article_title,
                        a.is_published,
                        c.slug     AS city_slug
                    FROM top_restaurants t
                    JOIN restaurants r   ON r.place_id = t.place_id
                    JOIN gemini_enrichments e ON e.place_id = r.place_id
                    JOIN cities c        ON c.id = t.city_id
                    LEFT JOIN editorial_articles a ON a.place_id = r.place_id
                    WHERE t.city_id = %s
                      AND e.cuisine_type = ANY(%s)
                    ORDER BY r.rating DESC NULLS LAST, r.rating_count DESC NULLS LAST
                    LIMIT 5
                """, (city_id, lbl["cuisine_types"]))
                lbl["restaurants"] = [dict(r) for r in cur.fetchall()]

            ctx["labels"] = [l for l in labels if l["restaurants"]]

            # Ausreißer: top_restaurants whose cuisine_type doesn't match any label
            all_types = []
            for l in labels:
                all_types.extend(l["cuisine_types"])
            if all_types:
                cur.execute("""
                    SELECT
                        r.place_id, r.name, r.address, r.rating, r.rating_count,
                        r.thumbnail_url, r.website, r.latitude, r.longitude,
                        e.cuisine_type, e.avg_price_pp,
                        a.slug     AS article_slug,
                        a.title    AS article_title,
                        a.is_published,
                        c.slug     AS city_slug
                    FROM top_restaurants t
                    JOIN restaurants r   ON r.place_id = t.place_id
                    JOIN gemini_enrichments e ON e.place_id = r.place_id
                    JOIN cities c        ON c.id = t.city_id
                    LEFT JOIN editorial_articles a ON a.place_id = r.place_id
                    WHERE t.city_id = %s
                      AND (e.cuisine_type IS NULL OR NOT (e.cuisine_type = ANY(%s)))
                    ORDER BY r.rating DESC NULLS LAST, r.rating_count DESC NULLS LAST
                    LIMIT 10
                """, (city_id, all_types))
                ctx["outliers"] = [dict(r) for r in cur.fetchall()]
            else:
                ctx["outliers"] = []

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    ctx["active_jobs"] = {
        job["place_id"]: {"job_id": jid, **job}
        for jid, job in _JOBS.items()
        if job.get("status") in ("pending", "running")
    }
    return render_template("kuechen.html", **ctx)


# ─────────────────────────────────────────────────────────────────────────────
# Articles list
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/articles")
def articles():
    city_id = request.args.get("city_id", 1, type=int)
    ctx: dict = {}
    try:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute("SELECT id, name, slug FROM cities ORDER BY id")
            ctx["cities"] = cur.fetchall()

            cur.execute("SELECT id, name, slug FROM cities WHERE id = %s", (city_id,))
            ctx["city"] = cur.fetchone()
            ctx["city_id"] = city_id

            cur.execute(
                """
                SELECT
                    r.place_id,
                    r.name,
                    r.address,
                    r.rating,
                    r.rating_count,
                    e.cuisine_type,
                    e.avg_price_pp,
                    a.slug          AS article_slug,
                    a.title         AS article_title,
                    a.teaser        AS article_teaser,
                    a.is_published,
                    a.generated_at,
                    c.slug          AS city_slug
                FROM top_restaurants t
                JOIN restaurants r ON r.place_id = t.place_id
                LEFT JOIN gemini_enrichments e ON e.place_id = r.place_id
                LEFT JOIN editorial_articles a ON a.place_id = r.place_id
                JOIN cities c ON c.id = t.city_id
                WHERE t.city_id = %s
                ORDER BY
                    a.is_published DESC NULLS LAST,
                    a.generated_at DESC NULLS LAST,
                    r.rating DESC NULLS LAST
                """,
                (city_id,),
            )
            ctx["restaurants"] = cur.fetchall()

            ctx["count_total"]     = len(ctx["restaurants"])
            ctx["count_published"] = sum(1 for r in ctx["restaurants"] if r["is_published"])
            ctx["count_draft"]     = sum(1 for r in ctx["restaurants"] if r["article_slug"] and not r["is_published"])
            ctx["count_none"]      = sum(1 for r in ctx["restaurants"] if not r["article_slug"])

        conn.close()
    except Exception as exc:
        ctx["error"] = str(exc)

    ctx["now"] = datetime.now(timezone.utc)
    # Pass active jobs so the template can restore spinners after page reload
    ctx["active_jobs"] = {
        job["place_id"]: {"job_id": jid, **job}
        for jid, job in _JOBS.items()
        if job.get("status") in ("pending", "running")
    }
    return render_template("articles.html", **ctx)


# ─────────────────────────────────────────────────────────────────────────────
# Start article generation (returns JSON with job_id)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/articles/generate", methods=["POST"])
def articles_generate():
    place_id = request.form["place_id"]
    name     = request.form["name"]
    address  = request.form["address"]
    city_id  = request.form.get("city_id", 1, type=int)

    # Kill any existing running job for this restaurant
    for jid, job in list(_JOBS.items()):
        if job.get("place_id") == place_id and job.get("status") in ("pending", "running"):
            _JOBS[jid]["status"] = "cancelled"

    job_id = str(uuid.uuid4())
    _JOBS[job_id] = {
        "status": "pending",
        "place_id": place_id,
        "name": name,
        "gemini_status": None,
        "error": None,
    }

    t = threading.Thread(
        target=_run_generation,
        args=(job_id, place_id, name, address, city_id),
        daemon=True,
    )
    t.start()

    return jsonify({"job_id": job_id, "status": "pending"})


# ─────────────────────────────────────────────────────────────────────────────
# Poll job status (JSON)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/articles/status/<job_id>")
def articles_status(job_id):
    job = _JOBS.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)


# ─────────────────────────────────────────────────────────────────────────────
# Publish / unpublish
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/articles/<place_id>/publish", methods=["POST"])
def articles_publish(place_id):
    action  = request.form.get("action", "publish")
    city_id = request.form.get("city_id", 1, type=int)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE editorial_articles SET is_published = %s WHERE place_id = %s",
                (action == "publish", place_id),
            )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("articles", city_id=city_id))


# ─────────────────────────────────────────────────────────────────────────────
# Delete article
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/articles/<place_id>/delete", methods=["POST"])
def articles_delete(place_id):
    city_id = request.form.get("city_id", 1, type=int)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM editorial_articles WHERE place_id = %s", (place_id,))
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("articles", city_id=city_id))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=False)

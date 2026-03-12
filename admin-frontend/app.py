"""
mallorcaeat Admin Frontend — Pipeline Status Dashboard
Runs on port 8081 (separate from main frontend on 8080).
"""

import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from flask import Flask, flash, redirect, render_template, request, url_for

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=False)

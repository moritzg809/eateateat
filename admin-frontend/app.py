"""
mallorcaeat Admin Frontend — Pipeline Status Dashboard
Runs on port 8081 (separate from main frontend on 8080).
"""

import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from flask import Flask, render_template

app = Flask(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]


def get_db():
    return psycopg2.connect(DATABASE_URL)


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=False)

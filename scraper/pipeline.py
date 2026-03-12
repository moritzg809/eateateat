"""
mallorcaeat pipeline — Unified end-to-end orchestrator

Stages (all idempotent — safe to restart at any time):
  1. search       Run Serper searches for queries due (> 6 months old)
  2. qualify      Mark below-threshold restaurants as 'disqualified'
  3. enrich       Gemini-enrich 'new' candidates (respects 500/day cap)
  4. completeness Mark 'enriched' restaurants as 'complete' if vibe+summary_de present
  2.5 gem_qualify Pre-qualify disqualified restaurants (rating ≥ 4.0) via short Gemini call
                  Only runs when no primary candidates pending AND quota remains
                  Qualifying candidates appear in the admin review queue
  5. details      Fetch SerpAPI details for 'complete' restaurants
  6. verify       Re-check 'complete' restaurants older than 2 years

Usage:
    python pipeline.py                          # full run (all stages)
    python pipeline.py --stages search,enrich   # specific stages only
    python pipeline.py --dry-run                # preview without API calls
    python pipeline.py --force-search           # bypass 6-month TTL on search
    python pipeline.py --stages verify          # re-verify old entries only
    python pipeline.py --daily-limit 200        # override enrichment daily cap
"""

import argparse
import logging
import time

import psycopg2.extras

import detail_scrape
import enrich as enricher
import gem_qualify
import scrape
from config import LOCATIONS, SEARCH_TERMS
from db import (
    count_today_enrichments,
    count_pending_new,
    fetch_for_verify,
    get_connection,
    init_pipeline_runs,
    set_pipeline_status,
    set_pipeline_status_force,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ALL_STAGES = ["search", "qualify", "enrich", "completeness", "gem_qualify", "details", "verify"]

# Quality thresholds (must match config)
MIN_RATING  = 4.5
MIN_REVIEWS = 100


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Search
# ─────────────────────────────────────────────────────────────────────────────

def stage_search(conn, dry_run: bool = False, force: bool = False):
    """Run Serper searches for queries that are due (> 6 months)."""
    logger.info("[SEARCH] Starting…")
    init_pipeline_runs(conn, SEARCH_TERMS, LOCATIONS)
    scrape.run(dry_run=dry_run, force=force)
    logger.info("[SEARCH] Done.")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: Qualify
# ─────────────────────────────────────────────────────────────────────────────

def stage_qualify(conn, dry_run: bool = False):
    """Mark new restaurants as 'disqualified' if they're below quality threshold."""
    logger.info("[QUALIFY] Checking new restaurants against quality threshold…")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT place_id, name, rating, rating_count
            FROM   restaurants
            WHERE  pipeline_status = 'new'
              AND  (rating < %s OR rating_count < %s)
            """,
            (MIN_RATING, MIN_REVIEWS),
        )
        to_disqualify = cur.fetchall()

    logger.info("[QUALIFY] %d restaurants below threshold", len(to_disqualify))

    if dry_run:
        for pid, name, rat, cnt in to_disqualify:
            logger.info("  WOULD disqualify: %s (%.1f★, %d reviews)", name, rat or 0, cnt or 0)
        return

    for pid, name, rat, cnt in to_disqualify:
        set_pipeline_status(conn, pid, "disqualified")

    # Also re-qualify 'disqualified' restaurants that now pass threshold
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE restaurants
            SET    pipeline_status = 'new'
            WHERE  pipeline_status = 'disqualified'
              AND  rating       >= %s
              AND  rating_count >= %s
            RETURNING place_id, name
            """,
            (MIN_RATING, MIN_REVIEWS),
        )
        requalified = cur.fetchall()
    conn.commit()

    if requalified:
        logger.info("[QUALIFY] %d previously disqualified restaurants now re-qualify", len(requalified))

    logger.info("[QUALIFY] Done. Disqualified: %d | Re-qualified: %d",
                len(to_disqualify), len(requalified))


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3: Enrich
# ─────────────────────────────────────────────────────────────────────────────

def stage_enrich(conn, dry_run: bool = False, limit=None, daily_limit: int = 500):
    """Gemini-enrich 'new' candidates. Respects the daily cap."""
    today_count = count_today_enrichments(conn)
    remaining = daily_limit - today_count

    if remaining <= 0:
        logger.info("[ENRICH] Daily limit reached (%d/%d) — skipping.", today_count, daily_limit)
        return

    effective_limit = limit if (limit is not None and limit <= remaining) else remaining
    logger.info("[ENRICH] Starting (today: %d/%d, will process up to %d)…",
                today_count, daily_limit, effective_limit)

    enricher.run(
        limit=effective_limit,
        min_rating=MIN_RATING,
        min_reviews=MIN_REVIEWS,
        dry_run=dry_run,
        force=False,
        daily_limit=daily_limit,
    )
    logger.info("[ENRICH] Done.")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4: Completeness
# ─────────────────────────────────────────────────────────────────────────────

def stage_completeness(conn, dry_run: bool = False):
    """
    Check 'enriched' restaurants for completeness.
    A restaurant is 'complete' if it has vibe, summary_de, and >= 5 scores.
    """
    logger.info("[COMPLETENESS] Checking enriched restaurants…")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                r.place_id,
                r.name,
                e.vibe,
                e.summary_de,
                (
                    (CASE WHEN e.family_score    IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.date_score      IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.friends_score   IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.solo_score      IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.relaxed_score   IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.party_score     IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.special_score   IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.foodie_score    IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.lingering_score IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.unique_score    IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN e.dresscode_score IS NOT NULL THEN 1 ELSE 0 END)
                ) AS score_count
            FROM restaurants r
            JOIN gemini_enrichments e ON e.place_id = r.place_id
            WHERE r.pipeline_status = 'enriched'
            """
        )
        rows = cur.fetchall()

    complete, incomplete = [], []
    for row in rows:
        if row["vibe"] and row["summary_de"] and row["score_count"] >= 5:
            complete.append(row["place_id"])
        else:
            incomplete.append((row["name"], row["vibe"], row["summary_de"], row["score_count"]))

    logger.info("[COMPLETENESS] %d complete / %d incomplete (out of %d enriched)",
                len(complete), len(incomplete), len(rows))

    if dry_run:
        for name, vibe, summ, sc in incomplete[:5]:
            logger.info("  INCOMPLETE: %s (vibe=%s, summary=%s, scores=%d)",
                        name, bool(vibe), bool(summ), sc)
        return

    for place_id in complete:
        set_pipeline_status(conn, place_id, "complete")

    logger.info("[COMPLETENESS] Done. Promoted to complete: %d", len(complete))


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2.5: Gem Qualify
# ─────────────────────────────────────────────────────────────────────────────

def stage_gem_qualify(conn, dry_run: bool = False, limit=None, daily_limit: int = 500):
    """
    Pre-qualify disqualified restaurants (rating >= 4.0) via a short Gemini call.
    Only runs when no primary candidates are pending enrichment AND there is
    remaining daily Gemini quota (enrich + prequalify share the 500/day cap).
    Qualifying restaurants appear in the admin review queue.
    """
    # Skip if primary enrichment work is still pending
    pending = count_pending_new(conn, MIN_RATING, MIN_REVIEWS)
    if pending > 0:
        logger.info(
            "[GEM_QUALIFY] %d primary candidate(s) still pending enrichment — skipping.",
            pending,
        )
        return

    # Check remaining daily quota (both stages share the cap)
    today_enrich     = count_today_enrichments(conn)
    today_prequalify = gem_qualify.count_today_prequalify(conn)
    total_today      = today_enrich + today_prequalify
    remaining        = daily_limit - total_today

    if remaining <= 0:
        logger.info(
            "[GEM_QUALIFY] Daily cap reached (%d/%d used) — skipping.",
            total_today, daily_limit,
        )
        return

    effective_limit = limit if (limit is not None and limit <= remaining) else remaining
    logger.info(
        "[GEM_QUALIFY] Starting (quota used today: %d/%d, will process up to %d)…",
        total_today, daily_limit, effective_limit,
    )
    gem_qualify.run(limit=effective_limit, dry_run=dry_run)
    logger.info("[GEM_QUALIFY] Done.")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 5: Details
# ─────────────────────────────────────────────────────────────────────────────

def stage_details(conn, dry_run: bool = False, limit=None):
    """Fetch SerpAPI place details for complete restaurants that don't have them yet."""
    logger.info("[DETAILS] Starting…")
    detail_scrape.run(
        limit=limit,
        min_rating=MIN_RATING,
        min_reviews=MIN_REVIEWS,
        dry_run=dry_run,
        force=False,
    )
    logger.info("[DETAILS] Done.")


# ─────────────────────────────────────────────────────────────────────────────
# Stage 6: Verify
# ─────────────────────────────────────────────────────────────────────────────

def stage_verify(conn, dry_run: bool = False, max_age_days: int = 730, limit=None):
    """
    Re-verify complete restaurants older than max_age_days (default 2 years).
    Re-fetches SerpAPI details, checks for closed status, re-checks rating threshold.
    """
    logger.info("[VERIFY] Looking for restaurants to re-check (> %d days old)…", max_age_days)

    rows = fetch_for_verify(conn, max_age_days)
    if limit:
        rows = rows[:limit]
    total = len(rows)

    if not rows:
        logger.info("[VERIFY] Nothing to verify.")
        return

    logger.info("[VERIFY] %d restaurants due for re-verification", total)

    if dry_run:
        for _, place_id, name, _, _ in rows[:10]:
            logger.info("  WOULD VERIFY: %s", name)
        return

    stats = {"ok": 0, "closed": 0, "disqualified": 0, "errors": 0}

    for i, (rid, place_id, name, address, data_cid) in enumerate(rows, 1):
        prefix = f"[{i:>3}/{total}]"
        logger.info("%s VERIFY %s", prefix, name)

        try:
            place, raw_response = detail_scrape.fetch_place_details(data_cid)
            detail_scrape.save_details(conn, place_id, place, raw_response)

            # Check closed
            if detail_scrape.is_place_closed(place):
                set_pipeline_status_force(conn, place_id, "inactive")
                logger.info("         -> 🚫 CLOSED — marked inactive")
                stats["closed"] += 1
                continue

            # Re-check quality threshold
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rating, rating_count FROM restaurants WHERE place_id = %s",
                    (place_id,),
                )
                row = cur.fetchone()
            if row:
                rating, rating_count = row
                if (rating is None or rating < MIN_RATING
                        or rating_count is None or rating_count < MIN_REVIEWS):
                    set_pipeline_status_force(conn, place_id, "disqualified")
                    logger.info("         -> ⬇ Below threshold (%.1f★, %d reviews) — disqualified",
                                rating or 0, rating_count or 0)
                    stats["disqualified"] += 1
                    continue

            # All good — update verified_at
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE restaurants SET last_verified_at = NOW() WHERE place_id = %s",
                    (place_id,),
                )
            conn.commit()
            logger.info("         -> ✓  verified ok")
            stats["ok"] += 1
            time.sleep(0.5)

        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    logger.info("[VERIFY] Done. OK: %d | Closed: %d | Disqualified: %d | Errors: %d",
                stats["ok"], stats["closed"], stats["disqualified"], stats["errors"])


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="mallorcaeat pipeline — unified end-to-end scraping orchestrator"
    )
    ap.add_argument(
        "--stages",
        default=",".join(ALL_STAGES),
        help=f"Comma-separated stages to run (default: all). Options: {', '.join(ALL_STAGES)}",
    )
    ap.add_argument("--dry-run",      action="store_true", help="Preview without API calls")
    ap.add_argument("--force-search", action="store_true", help="Bypass 6-month TTL on search stage")
    ap.add_argument("--daily-limit",  type=int, default=500, help="Max Gemini enrichments per day (default: 500)")
    ap.add_argument("--limit",        type=int, default=None, help="Max items per stage (for testing)")
    ap.add_argument("--verify-days",  type=int, default=730, help="Re-verify restaurants older than N days (default: 730)")
    args = ap.parse_args()

    stages = [s.strip().lower() for s in args.stages.split(",")]
    invalid = [s for s in stages if s not in ALL_STAGES]
    if invalid:
        ap.error(f"Unknown stage(s): {invalid}. Valid: {ALL_STAGES}")

    conn = get_connection()

    logger.info("=" * 60)
    logger.info("mallorcaeat pipeline")
    logger.info("  stages      : %s", ", ".join(stages))
    logger.info("  dry-run     : %s", args.dry_run)
    logger.info("  daily-limit : %d enrichments/day", args.daily_limit)
    logger.info("=" * 60)

    if "search" in stages:
        stage_search(conn, dry_run=args.dry_run, force=args.force_search)

    if "qualify" in stages:
        stage_qualify(conn, dry_run=args.dry_run)

    if "enrich" in stages:
        stage_enrich(conn, dry_run=args.dry_run, limit=args.limit, daily_limit=args.daily_limit)

    if "completeness" in stages:
        stage_completeness(conn, dry_run=args.dry_run)

    if "gem_qualify" in stages:
        stage_gem_qualify(conn, dry_run=args.dry_run, limit=args.limit, daily_limit=args.daily_limit)

    if "details" in stages:
        stage_details(conn, dry_run=args.dry_run, limit=args.limit)

    if "verify" in stages:
        stage_verify(conn, dry_run=args.dry_run, max_age_days=args.verify_days, limit=args.limit)

    conn.close()
    logger.info("=" * 60)
    logger.info("Pipeline complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

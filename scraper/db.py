import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ------------------------------------------------------------------
# Cache helpers
# ------------------------------------------------------------------

def get_cached(conn, query: str, location: str, search_type: str = "maps"):
    """Return (cache_id, response) if a cached entry exists, else (None, None)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, response
            FROM serper_cache
            WHERE query = %s AND location = %s AND search_type = %s
            """,
            (query, location, search_type),
        )
        row = cur.fetchone()
        if row:
            return row[0], row[1]
    return None, None


def save_cache(conn, query: str, location: str, search_type: str, response: dict) -> int:
    """Insert or replace a cache entry. Returns the cache row id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO serper_cache (query, location, search_type, response)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (query, location, search_type)
            DO UPDATE SET response = EXCLUDED.response, updated_at = NOW()
            RETURNING id
            """,
            (query, location, search_type, psycopg2.extras.Json(response)),
        )
        cache_id = cur.fetchone()[0]
    conn.commit()
    return cache_id


# ------------------------------------------------------------------
# Restaurant helpers
# ------------------------------------------------------------------

def upsert_restaurant(conn, place: dict) -> int | None:
    """
    Insert or update a restaurant from a Serper place object.
    Returns the restaurant row id, or None if place has no identifier.
    """
    place_id = place.get("placeId") or place.get("cid")
    if not place_id:
        logger.warning("Skipping place without id: %s", place.get("title"))
        return None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO restaurants (
                place_id, name, address, rating, rating_count,
                categories, phone, website, latitude, longitude,
                thumbnail_url, price_level, raw_data
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (place_id) DO UPDATE SET
                name          = EXCLUDED.name,
                address       = EXCLUDED.address,
                rating        = EXCLUDED.rating,
                rating_count  = EXCLUDED.rating_count,
                categories    = EXCLUDED.categories,
                phone         = EXCLUDED.phone,
                website       = EXCLUDED.website,
                latitude      = EXCLUDED.latitude,
                longitude     = EXCLUDED.longitude,
                thumbnail_url = EXCLUDED.thumbnail_url,
                price_level   = EXCLUDED.price_level,
                raw_data      = EXCLUDED.raw_data,
                updated_at    = NOW()
            RETURNING id
            """,
            (
                place_id,
                place.get("title"),
                place.get("address"),
                place.get("rating"),
                place.get("ratingCount"),
                place.get("categories", []),
                place.get("phoneNumber"),
                place.get("website"),
                place.get("latitude"),
                place.get("longitude"),
                place.get("thumbnailUrl"),
                place.get("priceLevel"),
                psycopg2.extras.Json(place),
            ),
        )
        restaurant_id = cur.fetchone()[0]
    conn.commit()
    return restaurant_id


def link_search_result(conn, cache_id: int, restaurant_id: int, position: int):
    """Record that a restaurant appeared at a given position in a search."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO search_results (cache_id, restaurant_id, position)
            VALUES (%s, %s, %s)
            ON CONFLICT (cache_id, restaurant_id) DO NOTHING
            """,
            (cache_id, restaurant_id, position),
        )
    conn.commit()


# ------------------------------------------------------------------
# Pipeline status helpers
# ------------------------------------------------------------------

def set_pipeline_status(conn, place_id: str, status: str):
    """Update a restaurant's pipeline_status without downgrading."""
    # Status priority order — never downgrade a complete restaurant to enriched etc.
    priority = {"new": 0, "disqualified": 1, "enriched": 2, "complete": 3, "inactive": 4}
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE restaurants
            SET pipeline_status  = %s,
                last_verified_at = CASE WHEN %s IN ('complete','inactive') THEN NOW()
                                        ELSE last_verified_at END
            WHERE place_id = %s
              AND (
                -- Only allow the update if the new status has higher priority
                -- or equal (to allow re-setting same stage, e.g. enriched→enriched)
                CASE pipeline_status
                    WHEN 'new'          THEN 0
                    WHEN 'disqualified' THEN 1
                    WHEN 'enriched'     THEN 2
                    WHEN 'complete'     THEN 3
                    WHEN 'inactive'     THEN 4
                    ELSE 0
                END <= %s
              )
            """,
            (status, status, place_id, priority.get(status, 0)),
        )
    conn.commit()


def set_pipeline_status_force(conn, place_id: str, status: str):
    """Force-update pipeline_status regardless of current value (e.g. for verify)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE restaurants
            SET pipeline_status  = %s,
                last_verified_at = NOW()
            WHERE place_id = %s
            """,
            (status, place_id),
        )
    conn.commit()


# ------------------------------------------------------------------
# Pipeline runs (search query tracking)
# ------------------------------------------------------------------

def init_pipeline_runs(conn, terms: list[str], locations: list[str]):
    """
    Seed pipeline_runs table from config SEARCH_TERMS × LOCATIONS.
    Only inserts rows that don't already exist.
    """
    with conn.cursor() as cur:
        for term in terms:
            for location in locations:
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (query, location)
                    VALUES (%s, %s)
                    ON CONFLICT (query, location) DO NOTHING
                    """,
                    (term, location),
                )
    conn.commit()


def get_pipeline_runs(conn) -> list:
    """Return all pipeline_runs ordered by urgency (overdue first, then never-run, then recent)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                    WHEN last_run_at IS NULL THEN NULL
                    ELSE last_run_at + INTERVAL '6 months'
                END AS next_due_at
            FROM pipeline_runs
            ORDER BY
                CASE WHEN last_run_at IS NULL THEN 0
                     WHEN last_run_at < NOW() - INTERVAL '6 months' THEN 1
                     ELSE 2 END,
                last_run_at ASC NULLS FIRST
            """
        )
        return cur.fetchall()


def get_due_pipeline_runs(conn, force: bool = False) -> list:
    """
    Return (query, location) pairs that need a search refresh.
    Due = never run OR last_run_at older than 6 months.
    force=True returns ALL entries regardless of age.
    """
    with conn.cursor() as cur:
        if force:
            cur.execute("SELECT query, location FROM pipeline_runs ORDER BY last_run_at NULLS FIRST")
        else:
            cur.execute(
                """
                SELECT query, location
                FROM   pipeline_runs
                WHERE  last_run_at IS NULL
                   OR  last_run_at < NOW() - INTERVAL '6 months'
                ORDER  BY last_run_at NULLS FIRST
                """
            )
        return cur.fetchall()


def mark_pipeline_run(conn, query: str, location: str, result_count: int, status: str = "ok"):
    """Update last_run_at and result_count after a search completes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (query, location, last_run_at, result_count, status)
            VALUES (%s, %s, NOW(), %s, %s)
            ON CONFLICT (query, location) DO UPDATE SET
                last_run_at  = NOW(),
                result_count = EXCLUDED.result_count,
                status       = EXCLUDED.status
            """,
            (query, location, result_count, status),
        )
    conn.commit()


# ------------------------------------------------------------------
# Verification helpers
# ------------------------------------------------------------------

def fetch_for_verify(conn, max_age_days: int = 730) -> list:
    """
    Return complete/active restaurants whose last_verified_at is older than
    max_age_days (default 2 years) or NULL.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.id, r.place_id, r.name, r.address,
                   r.raw_data->>'cid' AS data_cid
            FROM   restaurants r
            WHERE  r.pipeline_status = 'complete'
              AND  r.is_active = TRUE
              AND  (
                r.last_verified_at IS NULL
                OR r.last_verified_at < NOW() - (%(days)s || ' days')::INTERVAL
              )
              AND  r.raw_data->>'cid' IS NOT NULL
            ORDER  BY r.last_verified_at NULLS FIRST
            """,
            {"days": max_age_days},
        )
        return cur.fetchall()


def count_today_enrichments(conn) -> int:
    """Return number of Gemini enrichments done today (for daily cap)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM gemini_enrichments WHERE enriched_at::date = CURRENT_DATE"
        )
        return cur.fetchone()[0]

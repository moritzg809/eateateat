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

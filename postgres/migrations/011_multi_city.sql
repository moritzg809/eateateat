-- Migration 011: Multi-city support for EatEatEat platform
--
-- Adds a 'cities' table and city_id foreign keys to restaurants,
-- collections, and editorial_articles. All existing data is back-filled
-- to Mallorca (city_id = 1).
-- Also updates the top_restaurants view to expose city_id.

-- ── 1. cities table ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cities (
    id           SERIAL PRIMARY KEY,
    slug         TEXT NOT NULL UNIQUE,
    name         TEXT NOT NULL,
    emoji        TEXT DEFAULT '🏙',
    subtitle     TEXT,
    country_code TEXT DEFAULT 'DE',
    is_published BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order   INTEGER DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cities_slug      ON cities(slug);
CREATE INDEX IF NOT EXISTS idx_cities_published ON cities(is_published, sort_order);

-- ── 2. Seed Mallorca as city id=1 ─────────────────────────────────────────────

INSERT INTO cities (id, slug, name, emoji, subtitle, country_code, is_published, sort_order)
VALUES (1, 'mallorca', 'Mallorca', '🌊',
        'Die besten Tische der Insel — kuratiert, bewertet, ehrlich.',
        'ES', TRUE, 1)
ON CONFLICT (slug) DO UPDATE SET is_published = TRUE;

-- Advance sequence so next INSERT gets id=2
SELECT setval('cities_id_seq', GREATEST(1, (SELECT MAX(id) FROM cities)));

-- ── 3. city_id → restaurants ──────────────────────────────────────────────────

ALTER TABLE restaurants
    ADD COLUMN IF NOT EXISTS city_id INTEGER REFERENCES cities(id);

-- Back-fill: all existing restaurants are Mallorca
UPDATE restaurants SET city_id = 1 WHERE city_id IS NULL;

ALTER TABLE restaurants ALTER COLUMN city_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_restaurants_city_id ON restaurants(city_id);

-- ── 4. city_id → collections ──────────────────────────────────────────────────

ALTER TABLE collections
    ADD COLUMN IF NOT EXISTS city_id INTEGER REFERENCES cities(id);

UPDATE collections SET city_id = 1 WHERE city_id IS NULL;

ALTER TABLE collections ALTER COLUMN city_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_collections_city_id
    ON collections(city_id, is_published, sort_order);

-- ── 5. city_id → editorial_articles ──────────────────────────────────────────

ALTER TABLE editorial_articles
    ADD COLUMN IF NOT EXISTS city_id INTEGER REFERENCES cities(id);

-- Back-fill via restaurant join
UPDATE editorial_articles ea
SET city_id = r.city_id
FROM restaurants r
WHERE ea.place_id = r.place_id
  AND ea.city_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_editorial_articles_city_id
    ON editorial_articles(city_id);

-- ── 6. Rebuild top_restaurants view to expose city_id ─────────────────────────
-- Identical quality gate as migration 003, with r.city_id added to SELECT.

CREATE OR REPLACE VIEW top_restaurants AS
SELECT
    r.id,
    r.place_id,
    r.name,
    r.address,
    r.rating,
    r.rating_count,
    r.categories,
    r.phone,
    r.website,
    r.latitude,
    r.longitude,
    r.price_level,
    r.thumbnail_url,
    r.city_id                         -- new: enables per-city filtering
FROM restaurants r
WHERE r.pipeline_status = 'complete'
  AND r.is_active = TRUE
  AND (
    -- No enrichment row yet → pass-through (future-proofing)
    NOT EXISTS (
        SELECT 1 FROM gemini_enrichments e
        WHERE e.place_id = r.place_id AND e.family_score IS NOT NULL
    )
    OR (
        -- Must have at least 2 categories scoring >= 8
        SELECT (
            CASE WHEN e.family_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.date_score      >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.friends_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.solo_score      >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.relaxed_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.party_score     >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.special_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.foodie_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.lingering_score >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.unique_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.dresscode_score >= 8 THEN 1 ELSE 0 END
        ) >= 2
        FROM gemini_enrichments e
        WHERE e.place_id = r.place_id
    )
  )
ORDER BY r.rating DESC, r.rating_count DESC;

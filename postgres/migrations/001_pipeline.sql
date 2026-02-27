-- =============================================================
-- Migration 001: Pipeline status tracking
-- Safe to run multiple times (IF NOT EXISTS / IF EXISTS guards)
-- =============================================================

-- ---- 1. New columns on restaurants --------------------------

ALTER TABLE restaurants
    ADD COLUMN IF NOT EXISTS pipeline_status  TEXT    NOT NULL DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS scraped_at       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS is_active        BOOLEAN NOT NULL DEFAULT TRUE;

-- Back-fill scraped_at from created_at for existing rows
UPDATE restaurants SET scraped_at = created_at WHERE scraped_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_restaurants_pipeline_status
    ON restaurants (pipeline_status);
CREATE INDEX IF NOT EXISTS idx_restaurants_is_active
    ON restaurants (is_active);

-- ---- 2. raw_response on enrichment tables -------------------

ALTER TABLE gemini_enrichments
    ADD COLUMN IF NOT EXISTS raw_response JSONB;

ALTER TABLE serpapi_details
    ADD COLUMN IF NOT EXISTS raw_response JSONB;

-- ---- 3. pipeline_runs table ---------------------------------

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            SERIAL PRIMARY KEY,
    query         TEXT NOT NULL,
    location      TEXT NOT NULL,
    last_run_at   TIMESTAMPTZ,
    result_count  INTEGER NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL DEFAULT 'pending',  -- 'pending'|'ok'|'error'
    UNIQUE (query, location)
);

-- Backfill pipeline_runs from existing serper_cache entries
INSERT INTO pipeline_runs (query, location, last_run_at, result_count, status)
SELECT query, location, updated_at, 0, 'ok'
FROM   serper_cache
ON CONFLICT (query, location) DO NOTHING;

-- ---- 4. Data migration: classify existing restaurants --------

-- First: disqualify below-threshold (rating or reviews too low)
UPDATE restaurants
SET    pipeline_status = 'disqualified'
WHERE  (rating < 4.5 OR rating_count < 100)
  AND  pipeline_status = 'new';

-- Mark 'enriched' for restaurants with Gemini data
UPDATE restaurants
SET    pipeline_status = 'enriched'
WHERE  place_id IN (SELECT place_id FROM gemini_enrichments)
  AND  pipeline_status = 'new';

-- Mark 'complete' for restaurants with full Gemini output (vibe + summary_de)
-- and at least 5 non-null scores
UPDATE restaurants
SET    pipeline_status  = 'complete',
       last_verified_at = NOW()
WHERE  place_id IN (
    SELECT place_id FROM gemini_enrichments
    WHERE  vibe IS NOT NULL
      AND  summary_de IS NOT NULL
      AND  (
        (CASE WHEN family_score    IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN date_score      IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN friends_score   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN solo_score      IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN relaxed_score   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN party_score     IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN special_score   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN foodie_score    IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN lingering_score IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN unique_score    IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN dresscode_score IS NOT NULL THEN 1 ELSE 0 END)
      ) >= 5
)
AND pipeline_status IN ('new', 'enriched');

-- ---- 5. Replace top_restaurants view ------------------------

DROP VIEW IF EXISTS top_restaurants;

CREATE VIEW top_restaurants AS
SELECT
    id,
    place_id,
    name,
    address,
    rating,
    rating_count,
    categories,
    phone,
    website,
    latitude,
    longitude,
    price_level,
    thumbnail_url
FROM restaurants
WHERE pipeline_status = 'complete'
  AND is_active = TRUE
ORDER BY rating DESC, rating_count DESC;

-- ---- 6. Verify counts (informational) -----------------------

SELECT pipeline_status, count(*) AS n
FROM   restaurants
GROUP  BY 1
ORDER  BY 2 DESC;

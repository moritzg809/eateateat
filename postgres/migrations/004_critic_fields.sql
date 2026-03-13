-- Migration 004: Critic enrichment fields
--
-- Adds critic-style quality scores, audience dimensions, price estimate,
-- and cuisine classification to gemini_enrichments.
--
-- New display scores (replace dresscode + party in frontend):
--   outdoor_score  →  🏡 Terrasse filter
--   view_score     →  🌅 Aussicht filter
--
-- Existing restaurants: backfill via  python critic_enrich.py --backfill
-- New restaurants:       automatically populated by the extended enrich.py prompt
--

ALTER TABLE gemini_enrichments
    -- Critic-style quality dimensions
    ADD COLUMN IF NOT EXISTS cuisine_score   SMALLINT CHECK (cuisine_score   BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS service_score   SMALLINT CHECK (service_score   BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS value_score     SMALLINT CHECK (value_score     BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS ambiance_score  SMALLINT CHECK (ambiance_score  BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS critic_score    SMALLINT CHECK (critic_score    BETWEEN 1 AND 10),

    -- Display scores (shown as filter pills in the frontend)
    ADD COLUMN IF NOT EXISTS outdoor_score   SMALLINT CHECK (outdoor_score   BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS view_score      SMALLINT CHECK (view_score      BETWEEN 1 AND 10),

    -- Audience/target-group dimensions (internal, not displayed)
    ADD COLUMN IF NOT EXISTS scene_score     SMALLINT CHECK (scene_score     BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS local_score     SMALLINT CHECK (local_score     BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS warmth_score    SMALLINT CHECK (warmth_score    BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS substance_score SMALLINT CHECK (substance_score BETWEEN 1 AND 10),
    ADD COLUMN IF NOT EXISTS audience_type   TEXT,

    -- Price estimate (internal, used for future price-range filtering)
    ADD COLUMN IF NOT EXISTS avg_price_pp    INTEGER,

    -- Cuisine classification (internal, for future clustering / search)
    ADD COLUMN IF NOT EXISTS cuisine_type    TEXT,
    ADD COLUMN IF NOT EXISTS cuisine_tags    TEXT[];

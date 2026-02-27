-- =============================================================
-- mallorcaeat database schema
-- =============================================================

-- Raw API response cache (prevents duplicate Serper API calls)
CREATE TABLE serper_cache (
    id          SERIAL PRIMARY KEY,
    query       TEXT NOT NULL,
    location    TEXT NOT NULL,
    search_type TEXT NOT NULL DEFAULT 'maps',   -- 'maps' | 'places' | 'reviews'
    response    JSONB NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (query, location, search_type)
);

-- Restaurants (populated / upserted from serper_cache)
CREATE TABLE restaurants (
    id            SERIAL PRIMARY KEY,
    place_id      TEXT UNIQUE NOT NULL,   -- Google placeId or cid
    name          TEXT NOT NULL,
    address       TEXT,
    rating        NUMERIC(3, 1),
    rating_count  INTEGER,
    categories    TEXT[],
    phone         TEXT,
    website       TEXT,
    latitude      NUMERIC(10, 7),
    longitude     NUMERIC(10, 7),
    thumbnail_url TEXT,
    price_level   TEXT,                   -- '$' | '$$' | '$$$' | '$$$$'
    raw_data      JSONB,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Which search result positions each restaurant appeared in
CREATE TABLE search_results (
    id            SERIAL PRIMARY KEY,
    cache_id      INTEGER NOT NULL REFERENCES serper_cache (id),
    restaurant_id INTEGER NOT NULL REFERENCES restaurants (id),
    position      INTEGER,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (cache_id, restaurant_id)
);

-- =============================================================
-- Indexes
-- =============================================================

CREATE INDEX idx_restaurants_rating       ON restaurants (rating);
CREATE INDEX idx_restaurants_rating_count ON restaurants (rating_count);
CREATE INDEX idx_restaurants_place_id     ON restaurants (place_id);

-- =============================================================
-- Trigger: keep updated_at current on restaurants
-- =============================================================

CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_restaurants_updated_at
    BEFORE UPDATE ON restaurants
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE TRIGGER trg_cache_updated_at
    BEFORE UPDATE ON serper_cache
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- =============================================================
-- Gemini enrichment cache (keyed by Google place_id)
-- place_id is permanent → we never pay twice for the same spot
-- =============================================================

CREATE TABLE gemini_enrichments (
    id              SERIAL PRIMARY KEY,
    place_id        TEXT UNIQUE NOT NULL,   -- Google Place ID = cache key

    -- 10 Reiseprofil-Scores (1 = schlecht, 10 = perfekt)
    family_score    SMALLINT CHECK (family_score    BETWEEN 1 AND 10),  -- 👨‍👩‍👧 Familie
    date_score      SMALLINT CHECK (date_score      BETWEEN 1 AND 10),  -- 💑 Date Night
    friends_score   SMALLINT CHECK (friends_score   BETWEEN 1 AND 10),  -- 👯 Friends Trip
    solo_score      SMALLINT CHECK (solo_score      BETWEEN 1 AND 10),  -- 🧍 Solo
    relaxed_score   SMALLINT CHECK (relaxed_score   BETWEEN 1 AND 10),  -- 😌 Entspannt
    party_score     SMALLINT CHECK (party_score     BETWEEN 1 AND 10),  -- 🎉 Party Vibe
    special_score   SMALLINT CHECK (special_score   BETWEEN 1 AND 10),  -- ✨ Besonderer Anlass
    foodie_score    SMALLINT CHECK (foodie_score    BETWEEN 1 AND 10),  -- 🍽️ Foodie
    lingering_score SMALLINT CHECK (lingering_score BETWEEN 1 AND 10),  -- ☕ Verweilen
    unique_score    SMALLINT CHECK (unique_score    BETWEEN 1 AND 10),  -- 💎 Geheimtipp
    dresscode_score SMALLINT CHECK (dresscode_score BETWEEN 1 AND 10),  -- 👔 Dress Code

    -- Freitexte (Deutsch)
    summary_de      TEXT,   -- 2-3 Sätze warum hingehen
    must_order      TEXT,   -- was bestellen
    vibe            TEXT,   -- ein Satz zur Atmosphäre

    -- Metadaten
    gemini_model    TEXT,
    enriched_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_enrichments_place_id ON gemini_enrichments (place_id);

-- =============================================================
-- View: top restaurants (Step 2 filter)
--   Tweak MIN_RATING / MIN_REVIEWS here or override in app
-- =============================================================

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
WHERE rating       >= 4.5
  AND rating_count >= 100
ORDER BY rating DESC, rating_count DESC;
